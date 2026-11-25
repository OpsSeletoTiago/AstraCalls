package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/appstate"
	"go.mau.fi/whatsmeow/proto/waSyncAction"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
	"google.golang.org/protobuf/proto"
)

// Etiquetas do WhatsApp Business (as "labels" que o aparelho mostra na conversa).
//
// Elas vivem no app state da conta (coleção `regular`), não em mensagem: criar,
// renomear, apagar e (des)etiquetar uma conversa é um patch de app state, e o
// aparelho e os outros dispositivos vinculados recebem na hora.
//
// O whatsmeow não guarda etiquetas em lugar nenhum: só emite eventos
// (LabelEdit / LabelAssociationChat). Por isso mantemos um espelho no banco
// principal, alimentado de dois jeitos:
//   - mudanças ao vivo: os eventos chegam e atualizam o espelho;
//   - o que já existia antes: um full sync da coleção `regular`, feito sob
//     demanda (primeiro uso da sessão ou POST .../labels/sync).
//
// O full sync inicial do pareamento NÃO emite eventos (o padrão do whatsmeow é
// EmitAppStateEventsOnFullSync=false), então sem esse sync explícito o espelho
// de uma sessão já pareada nasceria vazio.

const (
	// O WhatsApp Business aceita 20 etiquetas por conta e 20 cores (0..19).
	labelMaxPerAccount = 20
	labelMaxColor      = 19
	labelMaxNameRunes  = 100
	// Intervalo mínimo entre dois full syncs pedidos pela API na mesma sessão.
	labelSyncMinInterval = 30 * time.Second
	// Prazo de um full sync / de um envio de patch. Não segue o contexto do
	// request: um sync cortado no meio deixaria a coleção sem versão local.
	labelSyncTimeout = 90 * time.Second
	labelSendTimeout = 30 * time.Second
)

var (
	errLabelsOffline  = errors.New("sessão desconectada do WhatsApp")
	errLabelNotFound  = errors.New("etiqueta não encontrada")
	errLabelLimit     = errors.New("a conta já tem 20 etiquetas, que é o limite do WhatsApp Business")
	errLabelName      = errors.New("name obrigatório, com até 100 caracteres")
	errLabelColor     = errors.New("color deve ficar entre 0 e 19")
	errLabelBadTarget = errors.New("etiqueta de conversa só vale para contato ou grupo")
)

// waLabel é uma etiqueta da conta.
type waLabel struct {
	ID           string `json:"id"`
	Name         string `json:"name"`
	Color        int32  `json:"color"`
	PredefinedID int32  `json:"predefinedId,omitempty"` // etiquetas de fábrica do WhatsApp Business
	Deleted      bool   `json:"deleted,omitempty"`
	Chats        int    `json:"chats"` // quantas conversas têm a etiqueta
}

// labelChat é uma conversa etiquetada.
type labelChat struct {
	LabelID string
	ChatJID string
}

// ---------- espelho no banco principal ----------

// initLabelTables cria as tabelas do espelho de etiquetas (banco principal).
func initLabelTables(ctx context.Context, db *sql.DB) error {
	if _, err := db.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS wa_labels (
		session_id    TEXT NOT NULL,
		label_id      TEXT NOT NULL,
		name          TEXT NOT NULL DEFAULT '',
		color         INT NOT NULL DEFAULT 0,
		predefined_id INT NOT NULL DEFAULT 0,
		deleted       BOOLEAN NOT NULL DEFAULT false,
		updated_at    BIGINT NOT NULL,
		PRIMARY KEY (session_id, label_id)
	)`); err != nil {
		return err
	}
	if _, err := db.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS wa_label_chats (
		session_id TEXT NOT NULL,
		label_id   TEXT NOT NULL,
		chat_jid   TEXT NOT NULL,
		updated_at BIGINT NOT NULL,
		PRIMARY KEY (session_id, label_id, chat_jid)
	)`); err != nil {
		return err
	}
	_, _ = db.ExecContext(ctx, `CREATE INDEX IF NOT EXISTS wa_label_chats_chat ON wa_label_chats (session_id, chat_jid)`)
	// Quando o espelho foi lido por inteiro do WhatsApp pela última vez (0 = nunca).
	if _, err := db.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS wa_label_sync (
		session_id TEXT PRIMARY KEY,
		synced_at  BIGINT NOT NULL
	)`); err != nil {
		return err
	}
	// Sessão removida fora do Delete (ex.: descartada no restore) não deixa resto.
	for _, t := range []string{"wa_labels", "wa_label_chats", "wa_label_sync"} {
		_, _ = db.ExecContext(ctx, `DELETE FROM `+t+` WHERE session_id NOT IN (SELECT id FROM sessions)`)
	}
	return nil
}

func (s *sessionStore) upsertLabel(ctx context.Context, sid string, l waLabel) error {
	_, err := s.db.ExecContext(ctx, `INSERT INTO wa_labels (session_id, label_id, name, color, predefined_id, deleted, updated_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7)
		ON CONFLICT (session_id, label_id) DO UPDATE SET
			name = EXCLUDED.name, color = EXCLUDED.color, deleted = EXCLUDED.deleted, updated_at = EXCLUDED.updated_at,
			predefined_id = CASE WHEN EXCLUDED.predefined_id <> 0 THEN EXCLUDED.predefined_id ELSE wa_labels.predefined_id END`,
		sid, l.ID, l.Name, l.Color, l.PredefinedID, l.Deleted, time.Now().UnixMilli())
	if err == nil && l.Deleted {
		// etiqueta apagada some das conversas
		_, err = s.db.ExecContext(ctx, `DELETE FROM wa_label_chats WHERE session_id = $1 AND label_id = $2`, sid, l.ID)
	}
	return err
}

func (s *sessionStore) setChatLabel(ctx context.Context, sid, labelID, chatJID string, labeled bool) error {
	if !labeled {
		_, err := s.db.ExecContext(ctx, `DELETE FROM wa_label_chats WHERE session_id = $1 AND label_id = $2 AND chat_jid = $3`, sid, labelID, chatJID)
		return err
	}
	_, err := s.db.ExecContext(ctx, `INSERT INTO wa_label_chats (session_id, label_id, chat_jid, updated_at) VALUES ($1, $2, $3, $4)
		ON CONFLICT (session_id, label_id, chat_jid) DO UPDATE SET updated_at = EXCLUDED.updated_at`,
		sid, labelID, chatJID, time.Now().UnixMilli())
	return err
}

// replaceLabels troca o espelho inteiro da sessão pelo resultado de um full sync.
func (s *sessionStore) replaceLabels(ctx context.Context, sid string, labels []waLabel, chats []labelChat) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	now := time.Now().UnixMilli()
	if _, err = tx.ExecContext(ctx, `DELETE FROM wa_labels WHERE session_id = $1`, sid); err != nil {
		return err
	}
	if _, err = tx.ExecContext(ctx, `DELETE FROM wa_label_chats WHERE session_id = $1`, sid); err != nil {
		return err
	}
	deleted := map[string]bool{}
	for _, l := range labels {
		deleted[l.ID] = l.Deleted
		if _, err = tx.ExecContext(ctx, `INSERT INTO wa_labels (session_id, label_id, name, color, predefined_id, deleted, updated_at)
			VALUES ($1, $2, $3, $4, $5, $6, $7) ON CONFLICT (session_id, label_id) DO UPDATE SET
			name = EXCLUDED.name, color = EXCLUDED.color, predefined_id = EXCLUDED.predefined_id, deleted = EXCLUDED.deleted, updated_at = EXCLUDED.updated_at`,
			sid, l.ID, l.Name, l.Color, l.PredefinedID, l.Deleted, now); err != nil {
			return err
		}
	}
	for _, c := range chats {
		if deleted[c.LabelID] {
			continue
		}
		if _, err = tx.ExecContext(ctx, `INSERT INTO wa_label_chats (session_id, label_id, chat_jid, updated_at) VALUES ($1, $2, $3, $4)
			ON CONFLICT (session_id, label_id, chat_jid) DO NOTHING`, sid, c.LabelID, c.ChatJID, now); err != nil {
			return err
		}
	}
	if _, err = tx.ExecContext(ctx, `INSERT INTO wa_label_sync (session_id, synced_at) VALUES ($1, $2)
		ON CONFLICT (session_id) DO UPDATE SET synced_at = EXCLUDED.synced_at`, sid, now); err != nil {
		return err
	}
	return tx.Commit()
}

// clearLabels zera o espelho da sessão (conta trocada, desvinculada ou apagada).
func (s *sessionStore) clearLabels(ctx context.Context, sid string) error {
	var first error
	for _, t := range []string{"wa_labels", "wa_label_chats", "wa_label_sync"} {
		if _, err := s.db.ExecContext(ctx, `DELETE FROM `+t+` WHERE session_id = $1`, sid); err != nil && first == nil {
			first = err
		}
	}
	return first
}

func (s *sessionStore) labelsSyncedAt(ctx context.Context, sid string) (int64, error) {
	var at int64
	err := s.db.QueryRowContext(ctx, `SELECT synced_at FROM wa_label_sync WHERE session_id = $1`, sid).Scan(&at)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, nil
	}
	return at, err
}

const labelSelect = `SELECT l.label_id, l.name, l.color, l.predefined_id, l.deleted,
	(SELECT count(*) FROM wa_label_chats c WHERE c.session_id = l.session_id AND c.label_id = l.label_id)
	FROM wa_labels l WHERE l.session_id = $1`

func scanLabels(rows *sql.Rows) ([]waLabel, error) {
	defer rows.Close()
	out := []waLabel{}
	for rows.Next() {
		var l waLabel
		if err := rows.Scan(&l.ID, &l.Name, &l.Color, &l.PredefinedID, &l.Deleted, &l.Chats); err != nil {
			return nil, err
		}
		out = append(out, l)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	sortLabels(out)
	return out, nil
}

func (s *sessionStore) listLabels(ctx context.Context, sid string, withDeleted bool) ([]waLabel, error) {
	q := labelSelect
	if !withDeleted {
		q += ` AND NOT l.deleted`
	}
	rows, err := s.db.QueryContext(ctx, q, sid)
	if err != nil {
		return nil, err
	}
	return scanLabels(rows)
}

func (s *sessionStore) getLabel(ctx context.Context, sid, id string) (waLabel, error) {
	rows, err := s.db.QueryContext(ctx, labelSelect+` AND l.label_id = $2`, sid, id)
	if err != nil {
		return waLabel{}, err
	}
	ls, err := scanLabels(rows)
	if err != nil {
		return waLabel{}, err
	}
	if len(ls) == 0 || ls[0].Deleted {
		return waLabel{}, errLabelNotFound
	}
	return ls[0], nil
}

// labelsOfChat devolve as etiquetas de uma conversa, procurando por todos os
// JIDs sob os quais ela pode estar gravada (telefone e LID).
func (s *sessionStore) labelsOfChat(ctx context.Context, sid string, jids []string) ([]waLabel, error) {
	if len(jids) == 0 {
		return []waLabel{}, nil
	}
	args := []any{sid}
	marks := make([]string, len(jids))
	for i, j := range jids {
		args = append(args, j)
		marks[i] = "$" + strconv.Itoa(i+2)
	}
	rows, err := s.db.QueryContext(ctx, labelSelect+` AND NOT l.deleted AND l.label_id IN (
		SELECT c.label_id FROM wa_label_chats c WHERE c.session_id = $1 AND c.chat_jid IN (`+strings.Join(marks, ",")+`))`, args...)
	if err != nil {
		return nil, err
	}
	return scanLabels(rows)
}

func (s *sessionStore) chatsOfLabel(ctx context.Context, sid, labelID string) ([]string, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT chat_jid FROM wa_label_chats WHERE session_id = $1 AND label_id = $2 ORDER BY updated_at DESC`, sid, labelID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []string{}
	for rows.Next() {
		var j string
		if err := rows.Scan(&j); err != nil {
			return nil, err
		}
		out = append(out, j)
	}
	return out, rows.Err()
}

// labelAddressingCounts conta as conversas 1:1 etiquetadas por forma de endereço.
// É o que o próprio aparelho gravou — a melhor pista de como ele indexa conversa.
func (s *sessionStore) labelAddressingCounts(ctx context.Context, sid string) (lid, pn int, err error) {
	err = s.db.QueryRowContext(ctx, `SELECT
		count(*) FILTER (WHERE chat_jid LIKE '%@lid'),
		count(*) FILTER (WHERE chat_jid LIKE '%@s.whatsapp.net')
		FROM wa_label_chats WHERE session_id = $1`, sid).Scan(&lid, &pn)
	return
}

// ---------- regras puras ----------

// sortLabels ordena por id numérico (a ordem em que o aparelho as criou).
func sortLabels(ls []waLabel) {
	sort.SliceStable(ls, func(i, j int) bool {
		a, errA := strconv.Atoi(ls[i].ID)
		b, errB := strconv.Atoi(ls[j].ID)
		if errA == nil && errB == nil {
			return a < b
		}
		if (errA == nil) != (errB == nil) {
			return errA == nil
		}
		return ls[i].ID < ls[j].ID
	})
}

// nextLabelID devolve o próximo id livre. O aparelho numera as etiquetas em
// sequência; seguimos a mesma regra olhando TODAS as conhecidas, inclusive as
// apagadas, para nunca reaproveitar um id.
func nextLabelID(known []waLabel) string {
	maxID := 0
	for _, l := range known {
		if n, err := strconv.Atoi(l.ID); err == nil && n > maxID {
			maxID = n
		}
	}
	return strconv.Itoa(maxID + 1)
}

func cleanLabelName(name string) (string, error) {
	name = strings.TrimSpace(name)
	if name == "" || utf8.RuneCountInString(name) > labelMaxNameRunes {
		return "", errLabelName
	}
	return name, nil
}

func validLabelColor(c int32) bool { return c >= 0 && c <= labelMaxColor }

// findLabelByName acha uma etiqueta viva pelo nome, sem diferenciar maiúsculas.
func findLabelByName(ls []waLabel, name string) (waLabel, bool) {
	for _, l := range ls {
		if !l.Deleted && strings.EqualFold(strings.TrimSpace(l.Name), name) {
			return l, true
		}
	}
	return waLabel{}, false
}

func countLiveLabels(ls []waLabel) int {
	n := 0
	for _, l := range ls {
		if !l.Deleted {
			n++
		}
	}
	return n
}

// pickLabelAddressing decide sob qual endereço uma conversa 1:1 é etiquetada:
// "lid" ou "pn". O índice do app state leva o JID da conversa COMO O APARELHO A
// GUARDA; com o endereço errado o patch é aceito mas a etiqueta não aparece.
//
// Ordem: o que foi pedido explicitamente; depois a forma que o próprio aparelho
// usou nas conversas já etiquetadas; por fim, se o banco de conversas da conta
// já migrou para LID.
func pickLabelAddressing(asked string, lidSeen, pnSeen int, migrated bool) string {
	switch asked {
	case "lid", "pn":
		return asked
	}
	if lidSeen != pnSeen {
		if lidSeen > pnSeen {
			return "lid"
		}
		return "pn"
	}
	if migrated {
		return "lid"
	}
	return "pn"
}

func labelFromEdit(evt *events.LabelEdit) waLabel {
	a := evt.Action
	return waLabel{
		ID:           evt.LabelID,
		Name:         a.GetName(),
		Color:        a.GetColor(),
		PredefinedID: a.GetPredefinedID(),
		Deleted:      a.GetDeleted(),
	}
}

// buildLabelEditPatch monta o patch de criar/renomear/apagar. Igual ao
// appstate.BuildLabelEdit, mas preserva o predefinedID das etiquetas de fábrica.
func buildLabelEditPatch(l waLabel) appstate.PatchInfo {
	act := &waSyncAction.LabelEditAction{
		Name:    proto.String(l.Name),
		Color:   proto.Int32(l.Color),
		Deleted: proto.Bool(l.Deleted),
	}
	if l.PredefinedID != 0 {
		act.PredefinedID = proto.Int32(l.PredefinedID)
	}
	return appstate.PatchInfo{
		Type: appstate.WAPatchRegular,
		Mutations: []appstate.MutationInfo{{
			Index:   []string{appstate.IndexLabelEdit, l.ID},
			Version: 3,
			Value:   &waSyncAction.SyncActionValue{LabelEditAction: act},
		}},
	}
}

// labelWebhookData é o payload do evento `label`: uma etiqueta foi criada,
// renomeada, recolorida ou apagada (em qualquer dispositivo da conta).
func labelWebhookData(l waLabel) map[string]any {
	return map[string]any{"id": l.ID, "name": l.Name, "color": l.Color, "deleted": l.Deleted}
}

// chatLabelWebhookData é o payload do evento `chat_label`: uma conversa ganhou
// ou perdeu uma etiqueta (em qualquer dispositivo da conta). `chat` é o JID como
// o aparelho o guarda (pode ser LID); `phone` vem quando o telefone é conhecido.
func chatLabelWebhookData(chat types.JID, phone, labelID, labelName string, labeled bool) map[string]any {
	d := map[string]any{
		"chat":      chat.String(),
		"isGroup":   chat.Server == types.GroupServer,
		"labelId":   labelID,
		"labelName": labelName,
		"labeled":   labeled,
	}
	if phone != "" {
		d["phone"] = phone
	}
	return d
}

// ---------- coleta do full sync ----------

// labelSync guarda, por sessão, a trava do full sync e o balde que recolhe os
// eventos enquanto um full sync roda (para trocar o espelho de uma vez só).
type labelSync struct {
	run sync.Mutex // um full sync por vez na sessão

	mu      sync.Mutex
	collect bool
	labels  []waLabel
	chats   []labelChat
}

var labelSyncs sync.Map // session id -> *labelSync

func labelSyncFor(sid string) *labelSync {
	v, _ := labelSyncs.LoadOrStore(sid, &labelSync{})
	return v.(*labelSync)
}

func (ls *labelSync) begin() {
	ls.mu.Lock()
	ls.collect, ls.labels, ls.chats = true, nil, nil
	ls.mu.Unlock()
}

// take encerra a coleta e devolve o que foi recolhido.
func (ls *labelSync) take() ([]waLabel, []labelChat) {
	ls.mu.Lock()
	defer ls.mu.Unlock()
	l, c := ls.labels, ls.chats
	ls.collect, ls.labels, ls.chats = false, nil, nil
	return l, c
}

func (ls *labelSync) addLabel(l waLabel) bool {
	ls.mu.Lock()
	defer ls.mu.Unlock()
	if !ls.collect {
		return false
	}
	ls.labels = append(ls.labels, l)
	return true
}

func (ls *labelSync) addChat(c labelChat) bool {
	ls.mu.Lock()
	defer ls.mu.Unlock()
	if !ls.collect {
		return false
	}
	ls.chats = append(ls.chats, c)
	return true
}

// handleLabelEvent mantém o espelho de etiquetas em dia. Chamado para todo
// evento da sessão; ignora o que não é de etiqueta.
func (s *Session) handleLabelEvent(rawEvt any) {
	if s.mgr == nil || s.mgr.store == nil {
		return
	}
	ctx := context.Background()
	switch evt := rawEvt.(type) {
	case *events.LabelEdit:
		l := labelFromEdit(evt)
		if evt.FromFullSync && labelSyncFor(s.id).addLabel(l) {
			return
		}
		if err := s.mgr.store.upsertLabel(ctx, s.id, l); err != nil {
			s.log.Warn("etiqueta: falha ao gravar", "label", l.ID, "err", err)
		}
		if !evt.FromFullSync {
			s.dispatchWebhook("label", labelWebhookData(l))
		}
	case *events.LabelAssociationChat:
		if evt.JID.IsEmpty() {
			return
		}
		labeled := evt.Action.GetLabeled()
		if evt.FromFullSync {
			if !labeled || labelSyncFor(s.id).addChat(labelChat{LabelID: evt.LabelID, ChatJID: evt.JID.String()}) {
				return
			}
		}
		if err := s.mgr.store.setChatLabel(ctx, s.id, evt.LabelID, evt.JID.String(), labeled); err != nil {
			s.log.Warn("etiqueta: falha ao gravar conversa", "label", evt.LabelID, "err", err)
		}
		if !evt.FromFullSync {
			name := ""
			if l, err := s.mgr.store.getLabel(ctx, s.id, evt.LabelID); err == nil {
				name = l.Name
			}
			phone := ""
			if evt.JID.Server != types.GroupServer {
				phone, _ = s.resolvePeer(evt.JID.String())
			}
			s.dispatchWebhook("chat_label", chatLabelWebhookData(evt.JID, phone, evt.LabelID, name, labeled))
		}
	case *events.PairSuccess, *events.LoggedOut:
		// outra conta (ou nenhuma) neste dispositivo: o espelho antigo não vale mais
		if err := s.mgr.store.clearLabels(ctx, s.id); err != nil {
			s.log.Warn("etiqueta: falha ao limpar o espelho", "err", err)
		}
	}
}

// ---------- operações na conta ----------

func (s *Session) labelsClient() (*whatsmeow.Client, error) {
	cli := s.client
	if cli == nil || !cli.IsConnected() || !cli.IsLoggedIn() {
		return nil, errLabelsOffline
	}
	return cli, nil
}

// syncLabels lê do WhatsApp todas as etiquetas e conversas etiquetadas da conta
// e troca o espelho. É um full sync da coleção `regular` do app state.
func (s *Session) syncLabels(ctx context.Context) error {
	cli, err := s.labelsClient()
	if err != nil {
		return err
	}
	ls := labelSyncFor(s.id)
	ls.run.Lock()
	defer ls.run.Unlock()
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), labelSyncTimeout)
	defer cancel()

	ls.begin()
	// O whatsmeow só emite os eventos de um full sync com este flag ligado. Fica
	// ligado só durante este sync, para não mudar o pareamento das sessões.
	prev := cli.EmitAppStateEventsOnFullSync
	cli.EmitAppStateEventsOnFullSync = true
	err = cli.FetchAppState(ctx, appstate.WAPatchRegular, true, false)
	cli.EmitAppStateEventsOnFullSync = prev
	labels, chats := ls.take()
	if err != nil {
		return err
	}
	if err := s.mgr.store.replaceLabels(ctx, s.id, labels, chats); err != nil {
		return err
	}
	s.log.Info("etiquetas sincronizadas", "labels", len(labels), "chats", len(chats))
	return nil
}

// ensureLabelsSynced faz o primeiro full sync da sessão, se ainda não houve.
func (s *Session) ensureLabelsSynced(ctx context.Context) error {
	at, err := s.mgr.store.labelsSyncedAt(ctx, s.id)
	if err != nil {
		return err
	}
	if at > 0 {
		return nil
	}
	return s.syncLabels(ctx)
}

func (s *Session) sendLabelPatch(ctx context.Context, patch appstate.PatchInfo) error {
	cli, err := s.labelsClient()
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), labelSendTimeout)
	defer cancel()
	return cli.SendAppState(ctx, patch)
}

// createLabel cria a etiqueta; se já existe uma viva com o mesmo nome, devolve
// ela (created=false) em vez de duplicar.
func (s *Session) createLabel(ctx context.Context, name string, color int32) (l waLabel, created bool, err error) {
	if err = s.ensureLabelsSynced(ctx); err != nil {
		return
	}
	known, err := s.mgr.store.listLabels(ctx, s.id, true)
	if err != nil {
		return
	}
	if found, ok := findLabelByName(known, name); ok {
		return found, false, nil
	}
	if countLiveLabels(known) >= labelMaxPerAccount {
		return l, false, errLabelLimit
	}
	l = waLabel{ID: nextLabelID(known), Name: name, Color: color}
	if err = s.sendLabelPatch(ctx, buildLabelEditPatch(l)); err != nil {
		return
	}
	return l, true, s.mgr.store.upsertLabel(ctx, s.id, l)
}

// labelChatTarget resolve o JID sob o qual a conversa é etiquetada.
func (s *Session) labelChatTarget(ctx context.Context, chat types.JID, asked string) (types.JID, error) {
	switch chat.Server {
	case types.GroupServer:
		return chat, nil
	case types.DefaultUserServer, types.HiddenUserServer:
	default:
		return types.JID{}, errLabelBadTarget
	}
	chat = chat.ToNonAD()
	lidSeen, pnSeen, err := s.mgr.store.labelAddressingCounts(ctx, s.id)
	if err != nil {
		return types.JID{}, err
	}
	want := pickLabelAddressing(asked, lidSeen, pnSeen, s.client.Store.LIDMigrationTimestamp > 0)
	have := "pn"
	if chat.Server == types.HiddenUserServer {
		have = "lid"
	}
	if want == have {
		return chat, nil
	}
	// troca de forma só quando o par telefone/LID é conhecido; senão fica o pedido
	if want == "lid" {
		if lid, err := s.client.Store.LIDs.GetLIDForPN(ctx, chat); err == nil && lid.User != "" {
			return lid.ToNonAD(), nil
		}
	} else if pn, err := s.client.Store.LIDs.GetPNForLID(ctx, chat); err == nil && pn.User != "" {
		return pn.ToNonAD(), nil
	}
	return chat, nil
}

// setChatLabel aplica (ou tira) a etiqueta da conversa e devolve o JID usado.
func (s *Session) setChatLabel(ctx context.Context, chat types.JID, labelID, asked string, labeled bool) (types.JID, error) {
	if err := s.ensureLabelsSynced(ctx); err != nil {
		return types.JID{}, err
	}
	if _, err := s.mgr.store.getLabel(ctx, s.id, labelID); err != nil {
		return types.JID{}, err
	}
	target, err := s.labelChatTarget(ctx, chat, asked)
	if err != nil {
		return types.JID{}, err
	}
	if err := s.sendLabelPatch(ctx, appstate.BuildLabelChat(target, labelID, labeled)); err != nil {
		return types.JID{}, err
	}
	if !labeled {
		// tira também de um registro antigo sob o outro endereço da mesma conversa
		for _, alias := range s.chatAliases(chat) {
			if alias != target.String() {
				_ = s.mgr.store.setChatLabel(ctx, s.id, labelID, alias, false)
			}
		}
	}
	return target, s.mgr.store.setChatLabel(ctx, s.id, labelID, target.String(), labeled)
}

// ---------- HTTP ----------

func writeLabelError(w http.ResponseWriter, err error) {
	code := http.StatusBadGateway // falha vinda do WhatsApp
	switch {
	case errors.Is(err, errLabelNotFound):
		code = http.StatusNotFound
	case errors.Is(err, errLabelsOffline), errors.Is(err, errLabelLimit):
		code = http.StatusConflict
	case errors.Is(err, errLabelName), errors.Is(err, errLabelColor), errors.Is(err, errLabelBadTarget):
		code = http.StatusBadRequest
	case errors.Is(err, whatsmeow.ErrAppStateRateLimited):
		code = http.StatusTooManyRequests
	}
	writeJSON(w, code, map[string]string{"error": err.Error()})
}

type labelsResponse struct {
	SyncedAt int64     `json:"syncedAt"` // ms; quando o espelho foi lido por inteiro do WhatsApp
	Labels   []waLabel `json:"labels"`
}

func (s *server) writeLabels(w http.ResponseWriter, r *http.Request, sess *Session) {
	labels, err := s.sessions.store.listLabels(r.Context(), sess.id, r.URL.Query().Get("deleted") == "true")
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	at, _ := s.sessions.store.labelsSyncedAt(r.Context(), sess.id)
	writeJSON(w, http.StatusOK, labelsResponse{SyncedAt: at, Labels: labels})
}

// GET /api/sessions/{sid}/labels?deleted=  → etiquetas da conta
// Na primeira chamada da sessão lê as etiquetas do WhatsApp (full sync).
func (s *server) handleListLabels(w http.ResponseWriter, r *http.Request) {
	sess := s.sessionByID(w, r.PathValue("sid"))
	if sess == nil {
		return
	}
	if err := sess.ensureLabelsSynced(r.Context()); err != nil {
		writeLabelError(w, err)
		return
	}
	s.writeLabels(w, r, sess)
}

// POST /api/sessions/{sid}/labels/sync  → relê tudo do WhatsApp e devolve a lista
func (s *server) handleSyncLabels(w http.ResponseWriter, r *http.Request) {
	sess := s.sessionByID(w, r.PathValue("sid"))
	if sess == nil {
		return
	}
	at, err := s.sessions.store.labelsSyncedAt(r.Context(), sess.id)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	if time.Since(time.UnixMilli(at)) >= labelSyncMinInterval {
		if err := sess.syncLabels(r.Context()); err != nil {
			writeLabelError(w, err)
			return
		}
	}
	s.writeLabels(w, r, sess)
}

type labelBody struct {
	Name  *string `json:"name"`
	Color *int32  `json:"color"`
}

func readLabelBody(r *http.Request) (labelBody, error) {
	var b labelBody
	if err := json.NewDecoder(r.Body).Decode(&b); err != nil {
		return b, errors.New("payload inválido")
	}
	if b.Name != nil {
		name, err := cleanLabelName(*b.Name)
		if err != nil {
			return b, err
		}
		b.Name = &name
	}
	if b.Color != nil && !validLabelColor(*b.Color) {
		return b, errLabelColor
	}
	return b, nil
}

// POST /api/sessions/{sid}/labels { "name": "...", "color": 0..19 }
// 201 com a etiqueta criada; 200 se já existia uma com o mesmo nome.
func (s *server) handleCreateLabel(w http.ResponseWriter, r *http.Request) {
	sess := s.sessionByID(w, r.PathValue("sid"))
	if sess == nil {
		return
	}
	b, err := readLabelBody(r)
	if err == nil && b.Name == nil {
		err = errLabelName
	}
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	var color int32
	if b.Color != nil {
		color = *b.Color
	}
	l, created, err := sess.createLabel(r.Context(), *b.Name, color)
	if err != nil {
		writeLabelError(w, err)
		return
	}
	code := http.StatusOK
	if created {
		code = http.StatusCreated
	}
	writeJSON(w, code, l)
}

// PUT /api/sessions/{sid}/labels/{labelId} { "name"?: "...", "color"?: 0..19 }
// Campos ausentes mantêm o valor atual.
func (s *server) handleUpdateLabel(w http.ResponseWriter, r *http.Request) {
	sess := s.sessionByID(w, r.PathValue("sid"))
	if sess == nil {
		return
	}
	b, err := readLabelBody(r)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	if err := sess.ensureLabelsSynced(r.Context()); err != nil {
		writeLabelError(w, err)
		return
	}
	l, err := s.sessions.store.getLabel(r.Context(), sess.id, r.PathValue("labelId"))
	if err != nil {
		writeLabelError(w, err)
		return
	}
	if b.Name != nil {
		l.Name = *b.Name
	}
	if b.Color != nil {
		l.Color = *b.Color
	}
	if err := sess.sendLabelPatch(r.Context(), buildLabelEditPatch(l)); err != nil {
		writeLabelError(w, err)
		return
	}
	if err := s.sessions.store.upsertLabel(r.Context(), sess.id, l); err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, l)
}

// DELETE /api/sessions/{sid}/labels/{labelId}  → apaga a etiqueta da conta
func (s *server) handleDeleteLabel(w http.ResponseWriter, r *http.Request) {
	sess := s.sessionByID(w, r.PathValue("sid"))
	if sess == nil {
		return
	}
	if err := sess.ensureLabelsSynced(r.Context()); err != nil {
		writeLabelError(w, err)
		return
	}
	l, err := s.sessions.store.getLabel(r.Context(), sess.id, r.PathValue("labelId"))
	if err != nil {
		writeLabelError(w, err)
		return
	}
	l.Deleted = true
	if err := sess.sendLabelPatch(r.Context(), buildLabelEditPatch(l)); err != nil {
		writeLabelError(w, err)
		return
	}
	if err := s.sessions.store.upsertLabel(r.Context(), sess.id, l); err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"id": l.ID, "deleted": true})
}

// GET /api/sessions/{sid}/labels/{labelId}/chats  → conversas com a etiqueta
func (s *server) handleLabelChats(w http.ResponseWriter, r *http.Request) {
	sess := s.sessionByID(w, r.PathValue("sid"))
	if sess == nil {
		return
	}
	if err := sess.ensureLabelsSynced(r.Context()); err != nil {
		writeLabelError(w, err)
		return
	}
	l, err := s.sessions.store.getLabel(r.Context(), sess.id, r.PathValue("labelId"))
	if err != nil {
		writeLabelError(w, err)
		return
	}
	jids, err := s.sessions.store.chatsOfLabel(r.Context(), sess.id, l.ID)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	type chatRef struct {
		Chat  string `json:"chat"`
		Phone string `json:"phone,omitempty"`
		Name  string `json:"name,omitempty"`
	}
	out := make([]chatRef, 0, len(jids))
	for _, j := range jids {
		c := chatRef{Chat: j}
		if !strings.HasSuffix(j, "@"+types.GroupServer) {
			c.Phone, c.Name = sess.resolvePeer(j)
		}
		out = append(out, c)
	}
	writeJSON(w, http.StatusOK, map[string]any{"label": l, "chats": out})
}

// GET /api/sessions/{sid}/chats/{chatId}/labels  → etiquetas da conversa
func (s *server) handleChatLabels(w http.ResponseWriter, r *http.Request) {
	sess := s.sessionByID(w, r.PathValue("sid"))
	if sess == nil {
		return
	}
	chat, err := resolveRecipient(r.PathValue("chatId"))
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	if err := sess.ensureLabelsSynced(r.Context()); err != nil {
		writeLabelError(w, err)
		return
	}
	labels, err := s.sessions.store.labelsOfChat(r.Context(), sess.id, sess.chatAliases(chat.ToNonAD()))
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, labels)
}

// PUT    /api/sessions/{sid}/chats/{chatId}/labels/{labelId}?addressing=  → etiqueta a conversa
// DELETE /api/sessions/{sid}/chats/{chatId}/labels/{labelId}?addressing=  → tira a etiqueta
//
// addressing (opcional) força o endereço da conversa: "pn" (telefone) ou "lid".
// Sem ele, segue a forma que o aparelho já usa (ver pickLabelAddressing).
func (s *server) handleSetChatLabel(labeled bool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		sess := s.sessionByID(w, r.PathValue("sid"))
		if sess == nil {
			return
		}
		chat, err := resolveRecipient(r.PathValue("chatId"))
		if err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
			return
		}
		labelID := r.PathValue("labelId")
		target, err := sess.setChatLabel(r.Context(), chat, labelID, r.URL.Query().Get("addressing"), labeled)
		if err != nil {
			writeLabelError(w, err)
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"chat": target.String(), "labelId": labelID, "labeled": labeled})
	}
}
