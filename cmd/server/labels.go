package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
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
	"google.golang.org/protobuf/encoding/protojson"
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
//     demanda (primeiro uso da sessão, espelho velho ou POST .../labels/sync).
//
// O full sync inicial do pareamento NÃO emite eventos (o padrão do whatsmeow é
// EmitAppStateEventsOnFullSync=false), então sem esse sync explícito o espelho
// de uma sessão já pareada nasceria vazio. Pelo mesmo motivo o espelho tem
// validade: um full sync que o whatsmeow faça por conta própria (recuperação de
// app state) e uma mutação REMOVE não geram evento nenhum, e só reler corrige.
//
// O mesmo índice (`label_edit`) guarda também as LISTAS do WhatsApp (não lidas,
// favoritos, grupos…), diferenciadas pelo campo `type`. Elas aparecem na
// listagem completa, mas a API só altera e aplica etiquetas de usuário.

const (
	// O WhatsApp Business aceita 20 etiquetas por conta e 20 cores (0..19).
	labelMaxPerAccount = 20
	labelMaxColor      = 19
	labelMaxNameRunes  = 100
	// Intervalo mínimo entre dois full syncs pedidos pela API na mesma sessão.
	labelSyncMinInterval = 30 * time.Second
	// Depois de um full sync que falhou, quanto esperar antes de tentar de novo.
	// Sem isso cada request repetiria o full sync e o WhatsApp responderia 429.
	labelSyncBackoff = time.Minute
	// Prazo de um full sync / de um envio de patch. Não segue o contexto do
	// request: um sync cortado no meio deixaria a coleção sem versão local.
	labelSyncTimeout = 90 * time.Second
	labelSendTimeout = 30 * time.Second
	// Prazo das gravações do espelho feitas dentro do tratamento de eventos.
	labelEventDBTimeout = 5 * time.Second
	labelBodyMaxBytes   = 64 << 10
)

// labelMirrorMaxAge é a validade do espelho: passado esse tempo, o próximo uso
// relê tudo do WhatsApp. 0 desliga a releitura automática.
var labelMirrorMaxAge = time.Duration(envInt("WACALLS_LABELS_RESYNC_HOURS", 24)) * time.Hour

var (
	errLabelsOffline   = errors.New("sessão desconectada do WhatsApp")
	errLabelNotFound   = errors.New("etiqueta não encontrada")
	errLabelLimit      = errors.New("a conta já tem 20 etiquetas, que é o limite do WhatsApp Business")
	errLabelName       = errors.New("name obrigatório, com até 100 caracteres")
	errLabelColor      = errors.New("color deve ficar entre 0 e 19")
	errLabelAddressing = errors.New("addressing deve ser pn ou lid")
	errLabelBadTarget  = errors.New("etiqueta de conversa só vale para contato ou grupo")
	errLabelSystem     = errors.New("esta é uma lista do próprio WhatsApp e não pode ser alterada nem aplicada pela API")
	errLabelSyncWait   = errors.New("a última leitura das etiquetas falhou; aguarde antes de tentar de novo")
	errLabelNotReady   = errors.New("o app state da conta ainda não foi sincronizado; tente de novo em instantes")
	errLabelStore      = errors.New("falha no banco do espelho de etiquetas")
)

func labelStoreErr(err error) error {
	if err == nil {
		return nil
	}
	return fmt.Errorf("%w: %v", errLabelStore, err)
}

// waLabel é uma etiqueta (ou lista) da conta.
type waLabel struct {
	ID           string          `json:"id"`
	Name         string          `json:"name"`
	Color        int32           `json:"color"`
	PredefinedID int32           `json:"predefinedId,omitempty"` // etiquetas de fábrica do WhatsApp Business
	Type         string          `json:"type,omitempty"`         // custom, predefined, unread, favorites…
	Immutable    bool            `json:"immutable,omitempty"`
	Deleted      bool            `json:"deleted,omitempty"`
	Chats        int             `json:"chats"`            // quantas conversas têm a etiqueta
	Action       json.RawMessage `json:"action,omitempty"` // registro completo no app state (só com ?raw=true)

	// act é o registro completo como veio do app state. Um SET troca o valor
	// inteiro do índice, então toda edição parte dele para não apagar campos
	// que esta API não conhece (ordem, ativo, tipo…).
	act *waSyncAction.LabelEditAction
}

// labelChat é uma conversa etiquetada.
type labelChat struct {
	LabelID string
	ChatJID string
}

func newLabel(id string, act *waSyncAction.LabelEditAction) waLabel {
	l := waLabel{
		ID:           id,
		Name:         act.GetName(),
		Color:        act.GetColor(),
		PredefinedID: act.GetPredefinedID(),
		Immutable:    act.GetIsImmutable(),
		Deleted:      act.GetDeleted(),
		act:          act,
	}
	if act != nil && act.Type != nil {
		l.Type = strings.ToLower(act.GetType().String())
	}
	return l
}

// isUserLabel diz se é uma etiqueta de usuário (criada no aparelho ou de fábrica
// do Business), e não uma lista do próprio WhatsApp.
func (l waLabel) isUserLabel() bool {
	if l.act == nil || l.act.Type == nil {
		return true
	}
	switch l.act.GetType() {
	case waSyncAction.LabelEditAction_NONE, waSyncAction.LabelEditAction_PREDEFINED, waSyncAction.LabelEditAction_CUSTOM:
		return true
	}
	return false
}

// ---------- espelho no banco principal ----------

// initLabelTables cria as tabelas do espelho de etiquetas (banco principal).
func initLabelTables(ctx context.Context, db *sql.DB) error {
	if _, err := db.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS wa_labels (
		session_id    TEXT NOT NULL,
		label_id      TEXT NOT NULL,
		name          TEXT NOT NULL DEFAULT '',
		color         INT NOT NULL DEFAULT 0,
		deleted       BOOLEAN NOT NULL DEFAULT false,
		action        BYTEA,
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
	// Quando o espelho foi lido por inteiro do WhatsApp pela última vez. Sem
	// linha = nunca, ou a última leitura falhou e o espelho não é confiável.
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

func labelActionBytes(l waLabel) []byte {
	if l.act == nil {
		return nil
	}
	b, err := proto.Marshal(l.act)
	if err != nil {
		return nil
	}
	return b
}

const labelUpsertSQL = `INSERT INTO wa_labels (session_id, label_id, name, color, deleted, action, updated_at)
	VALUES ($1, $2, $3, $4, $5, $6, $7)
	ON CONFLICT (session_id, label_id) DO UPDATE SET
		name = EXCLUDED.name, color = EXCLUDED.color, deleted = EXCLUDED.deleted, updated_at = EXCLUDED.updated_at,
		action = COALESCE(EXCLUDED.action, wa_labels.action)`

func (s *sessionStore) upsertLabel(ctx context.Context, sid string, l waLabel) error {
	_, err := s.db.ExecContext(ctx, labelUpsertSQL, sid, l.ID, l.Name, l.Color, l.Deleted, labelActionBytes(l), time.Now().UnixMilli())
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
		if _, err = tx.ExecContext(ctx, labelUpsertSQL, sid, l.ID, l.Name, l.Color, l.Deleted, labelActionBytes(l), now); err != nil {
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

// markLabelsUnsynced tira a validade do espelho: a próxima leitura refaz tudo.
func (s *sessionStore) markLabelsUnsynced(ctx context.Context, sid string) error {
	_, err := s.db.ExecContext(ctx, `DELETE FROM wa_label_sync WHERE session_id = $1`, sid)
	return err
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

const labelSelect = `SELECT l.label_id, l.name, l.color, l.deleted, l.action,
	(SELECT count(*) FROM wa_label_chats c WHERE c.session_id = l.session_id AND c.label_id = l.label_id)
	FROM wa_labels l WHERE l.session_id = $1`

func scanLabels(rows *sql.Rows) ([]waLabel, error) {
	defer rows.Close()
	out := []waLabel{}
	for rows.Next() {
		var (
			id, name string
			color    int32
			deleted  bool
			raw      []byte
			chats    int
		)
		if err := rows.Scan(&id, &name, &color, &deleted, &raw, &chats); err != nil {
			return nil, err
		}
		var act *waSyncAction.LabelEditAction
		if len(raw) > 0 {
			act = &waSyncAction.LabelEditAction{}
			if err := proto.Unmarshal(raw, act); err != nil {
				act = nil
			}
		}
		l := newLabel(id, act)
		// as colunas valem mais que o registro: são o que a última escrita gravou
		l.Name, l.Color, l.Deleted, l.Chats = name, color, deleted, chats
		out = append(out, l)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	sortLabels(out)
	return out, nil
}

// listLabels devolve TUDO o que o espelho tem da sessão: etiquetas de usuário,
// listas do WhatsApp e as apagadas. Quem chama filtra.
func (s *sessionStore) listLabels(ctx context.Context, sid string) ([]waLabel, error) {
	rows, err := s.db.QueryContext(ctx, labelSelect, sid)
	if err != nil {
		return nil, err
	}
	return scanLabels(rows)
}

// getLabel devolve uma etiqueta viva (as apagadas contam como inexistentes).
func (s *sessionStore) getLabel(ctx context.Context, sid, id string) (waLabel, error) {
	rows, err := s.db.QueryContext(ctx, labelSelect+` AND l.label_id = $2`, sid, id)
	if err != nil {
		return waLabel{}, labelStoreErr(err)
	}
	ls, err := scanLabels(rows)
	if err != nil {
		return waLabel{}, labelStoreErr(err)
	}
	if len(ls) == 0 || ls[0].Deleted {
		return waLabel{}, errLabelNotFound
	}
	return ls[0], nil
}

func jidMarks(first int, jids []string, args []any) (string, []any) {
	marks := make([]string, len(jids))
	for i, j := range jids {
		args = append(args, j)
		marks[i] = "$" + strconv.Itoa(first+i)
	}
	return strings.Join(marks, ","), args
}

// labelsOfChat devolve as etiquetas de uma conversa, procurando por todos os
// JIDs sob os quais ela pode estar gravada (telefone e LID).
func (s *sessionStore) labelsOfChat(ctx context.Context, sid string, jids []string) ([]waLabel, error) {
	if len(jids) == 0 {
		return []waLabel{}, nil
	}
	marks, args := jidMarks(2, jids, []any{sid})
	rows, err := s.db.QueryContext(ctx, labelSelect+` AND NOT l.deleted AND l.label_id IN (
		SELECT c.label_id FROM wa_label_chats c WHERE c.session_id = $1 AND c.chat_jid IN (`+marks+`))`, args...)
	if err != nil {
		return nil, err
	}
	return scanLabels(rows)
}

// labeledAliases diz sob quais dos JIDs dados a conversa está com a etiqueta.
func (s *sessionStore) labeledAliases(ctx context.Context, sid, labelID string, jids []string) ([]string, error) {
	if len(jids) == 0 {
		return nil, nil
	}
	marks, args := jidMarks(3, jids, []any{sid, labelID})
	rows, err := s.db.QueryContext(ctx, `SELECT chat_jid FROM wa_label_chats WHERE session_id = $1 AND label_id = $2 AND chat_jid IN (`+marks+`)`, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var j string
		if err := rows.Scan(&j); err != nil {
			return nil, err
		}
		out = append(out, j)
	}
	return out, rows.Err()
}

func (s *sessionStore) chatsOfLabel(ctx context.Context, sid, labelID string, limit, offset int) ([]string, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT chat_jid FROM wa_label_chats WHERE session_id = $1 AND label_id = $2
		ORDER BY updated_at DESC, chat_jid LIMIT $3 OFFSET $4`, sid, labelID, limit, offset)
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
// sequência; seguimos a mesma regra olhando TUDO o que o índice tem (listas e
// apagadas também), para nunca reaproveitar um id.
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

// userLabels filtra as etiquetas de usuário vivas (o que a API lista por padrão).
func userLabels(ls []waLabel) []waLabel {
	out := make([]waLabel, 0, len(ls))
	for _, l := range ls {
		if !l.Deleted && l.isUserLabel() {
			out = append(out, l)
		}
	}
	return out
}

// findLabelByName acha uma etiqueta de usuário viva pelo nome, sem diferenciar maiúsculas.
func findLabelByName(ls []waLabel, name string) (waLabel, bool) {
	for _, l := range userLabels(ls) {
		if strings.EqualFold(strings.TrimSpace(l.Name), name) {
			return l, true
		}
	}
	return waLabel{}, false
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

func validLabelAddressing(a string) bool { return a == "" || a == "pn" || a == "lid" }

func labelFromEdit(evt *events.LabelEdit) waLabel { return newLabel(evt.LabelID, evt.Action) }

// newLabelAction monta o registro de uma etiqueta NOVA. Os campos além de nome
// e cor seguem os de uma etiqueta que o próprio aparelho criou nesta conta (o
// molde), para a nova ficar igual às dele; sem molde, vai só o essencial.
func newLabelAction(name string, color int32, known []waLabel) *waSyncAction.LabelEditAction {
	act := &waSyncAction.LabelEditAction{Name: proto.String(name), Color: proto.Int32(color), Deleted: proto.Bool(false)}
	var mold *waSyncAction.LabelEditAction
	maxOrder, hasOrder := int32(0), false
	for _, l := range known {
		if l.act == nil {
			continue
		}
		if l.act.OrderIndex != nil {
			hasOrder = true
			if o := l.act.GetOrderIndex(); o > maxOrder {
				maxOrder = o
			}
		}
		if !l.Deleted && l.act.GetType() == waSyncAction.LabelEditAction_CUSTOM {
			mold = l.act
		}
	}
	if mold != nil {
		act.Type = mold.Type
		act.IsActive = mold.IsActive
		if mold.OrderIndex != nil && hasOrder {
			act.OrderIndex = proto.Int32(maxOrder + 1)
		}
	}
	return act
}

// editedLabelAction devolve o registro da etiqueta com nome/cor/apagada novos,
// preservando todos os outros campos do registro atual.
func editedLabelAction(l waLabel) *waSyncAction.LabelEditAction {
	var act *waSyncAction.LabelEditAction
	if l.act != nil {
		act = proto.Clone(l.act).(*waSyncAction.LabelEditAction)
	} else {
		act = &waSyncAction.LabelEditAction{}
		if l.PredefinedID != 0 {
			act.PredefinedID = proto.Int32(l.PredefinedID)
		}
	}
	act.Name, act.Color, act.Deleted = proto.String(l.Name), proto.Int32(l.Color), proto.Bool(l.Deleted)
	return act
}

// buildLabelEditPatch monta o patch de criar/renomear/apagar a partir do
// registro completo (o appstate.BuildLabelEdit só leva nome, cor e apagada).
func buildLabelEditPatch(id string, act *waSyncAction.LabelEditAction) appstate.PatchInfo {
	return appstate.PatchInfo{
		Type: appstate.WAPatchRegular,
		Mutations: []appstate.MutationInfo{{
			Index:   []string{appstate.IndexLabelEdit, id},
			Version: 3,
			Value:   &waSyncAction.SyncActionValue{LabelEditAction: act},
		}},
	}
}

// buildLabelChatsPatch (des)etiqueta um ou mais JIDs com a mesma etiqueta num patch só.
func buildLabelChatsPatch(labelID string, targets []types.JID, labeled bool) appstate.PatchInfo {
	p := appstate.PatchInfo{Type: appstate.WAPatchRegular}
	for _, t := range targets {
		p.Mutations = append(p.Mutations, appstate.BuildLabelChat(t, labelID, labeled).Mutations...)
	}
	return p
}

// labelWebhookData é o payload do evento `label`: uma etiqueta foi criada,
// renomeada, recolorida ou apagada (em qualquer dispositivo da conta).
func labelWebhookData(l waLabel) map[string]any {
	d := map[string]any{"id": l.ID, "name": l.Name, "color": l.Color, "deleted": l.Deleted}
	if l.Type != "" {
		d["type"] = l.Type
	}
	return d
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

// ---------- estado por sessão ----------

// labelSync guarda, por sessão, a trava que serializa full sync e escritas, e o
// balde que recolhe os eventos enquanto um full sync roda.
type labelSync struct {
	// run serializa, na sessão, o full sync e toda escrita de etiqueta. Um patch
	// enviado no meio de um full sync leria a coleção sem versão local, e duas
	// criações simultâneas calculariam o mesmo id (a segunda sobrescreve a primeira).
	run sync.Mutex
	// protegidos por run:
	lastAttempt time.Time
	lastErr     error

	mu      sync.Mutex
	collect bool
	labels  map[string]waLabel
	chats   map[labelChat]bool
}

var labelSyncs sync.Map // session id -> *labelSync

func labelSyncFor(sid string) *labelSync {
	v, _ := labelSyncs.LoadOrStore(sid, &labelSync{})
	return v.(*labelSync)
}

func (ls *labelSync) begin() {
	ls.mu.Lock()
	ls.collect, ls.labels, ls.chats = true, map[string]waLabel{}, map[labelChat]bool{}
	ls.mu.Unlock()
}

// drain devolve o que foi recolhido e esvazia o balde; com stop, encerra a coleta.
// Vale o ÚLTIMO valor de cada chave: um full sync é o snapshot mais os patches
// seguintes, e um patch pode desfazer o que o snapshot dizia.
func (ls *labelSync) drain(stop bool) ([]waLabel, map[labelChat]bool) {
	ls.mu.Lock()
	defer ls.mu.Unlock()
	labels := make([]waLabel, 0, len(ls.labels))
	for _, l := range ls.labels {
		labels = append(labels, l)
	}
	chats := ls.chats
	if stop {
		ls.collect, ls.labels, ls.chats = false, nil, nil
	} else {
		ls.labels, ls.chats = map[string]waLabel{}, map[labelChat]bool{}
	}
	return labels, chats
}

func (ls *labelSync) addLabel(l waLabel) bool {
	ls.mu.Lock()
	defer ls.mu.Unlock()
	if !ls.collect {
		return false
	}
	ls.labels[l.ID] = l
	return true
}

func (ls *labelSync) addChat(c labelChat, labeled bool) bool {
	ls.mu.Lock()
	defer ls.mu.Unlock()
	if !ls.collect {
		return false
	}
	ls.chats[c] = labeled
	return true
}

func labeledChats(m map[labelChat]bool) []labelChat {
	out := make([]labelChat, 0, len(m))
	for c, on := range m {
		if on {
			out = append(out, c)
		}
	}
	return out
}

// handleLabelEvent mantém o espelho de etiquetas em dia. Chamado para todo
// evento da sessão; ignora na hora o que não é de etiqueta.
func (s *Session) handleLabelEvent(rawEvt any) {
	switch evt := rawEvt.(type) {
	case *events.LabelEdit:
		if s.mgr == nil || s.mgr.store == nil {
			return
		}
		l := labelFromEdit(evt)
		if !evt.FromFullSync {
			s.dispatchWebhook("label", labelWebhookData(l))
		}
		// Enquanto um full sync roda, tudo vai para o balde (na ordem em que
		// chega) e entra no espelho de uma vez, junto com o snapshot.
		if labelSyncFor(s.id).addLabel(l) {
			return
		}
		ctx, cancel := context.WithTimeout(context.Background(), labelEventDBTimeout)
		defer cancel()
		if err := s.mgr.store.upsertLabel(ctx, s.id, l); err != nil {
			s.log.Warn("etiqueta: falha ao gravar", "label", l.ID, "err", err)
		}
	case *events.LabelAssociationChat:
		if s.mgr == nil || s.mgr.store == nil || evt.JID.IsEmpty() {
			return
		}
		labeled := evt.Action.GetLabeled()
		chat := evt.JID.String()
		ctx, cancel := context.WithTimeout(context.Background(), labelEventDBTimeout)
		defer cancel()
		if !evt.FromFullSync {
			name := ""
			if l, err := s.mgr.store.getLabel(ctx, s.id, evt.LabelID); err == nil {
				name = l.Name
			}
			phone := ""
			if evt.JID.Server != types.GroupServer {
				phone, _ = s.resolvePeer(chat)
			}
			s.dispatchWebhook("chat_label", chatLabelWebhookData(evt.JID, phone, evt.LabelID, name, labeled))
		}
		if labelSyncFor(s.id).addChat(labelChat{LabelID: evt.LabelID, ChatJID: chat}, labeled) {
			return
		}
		if err := s.mgr.store.setChatLabel(ctx, s.id, evt.LabelID, chat, labeled); err != nil {
			s.log.Warn("etiqueta: falha ao gravar conversa", "label", evt.LabelID, "err", err)
		}
	case *events.PairSuccess, *events.LoggedOut:
		if s.mgr == nil || s.mgr.store == nil {
			return
		}
		// Outra conta (ou nenhuma) neste dispositivo: o espelho antigo não vale
		// mais. Fora do caminho do evento, para um banco lento não atrasar o
		// tratamento de login/logout que vem logo depois.
		store, sid, log := s.mgr.store, s.id, s.log
		go func() {
			ctx, cancel := context.WithTimeout(context.Background(), labelEventDBTimeout)
			defer cancel()
			if err := store.clearLabels(ctx, sid); err != nil {
				log.Warn("etiqueta: falha ao limpar o espelho", "err", err)
			}
		}()
	}
}

// dropLabels esquece tudo o que há de etiqueta de uma sessão apagada.
func (m *SessionManager) dropLabels(ctx context.Context, sid string) {
	_ = m.store.clearLabels(ctx, sid)
	labelSyncs.Delete(sid)
}

// ---------- operações na conta ----------

func (s *Session) labelsClient() (*whatsmeow.Client, error) {
	cli := s.client
	if cli == nil || !cli.IsConnected() || !cli.IsLoggedIn() {
		return nil, errLabelsOffline
	}
	return cli, nil
}

// lockLabels pega a trava de etiquetas da sessão. Quem chama deve soltá-la.
func (s *Session) lockLabels() *labelSync {
	ls := labelSyncFor(s.id)
	ls.run.Lock()
	return ls
}

func labelMirrorFresh(syncedAt int64) bool {
	if syncedAt <= 0 {
		return false
	}
	return labelMirrorMaxAge <= 0 || time.Since(time.UnixMilli(syncedAt)) < labelMirrorMaxAge
}

// syncLabelsLocked lê do WhatsApp todas as etiquetas e conversas etiquetadas da
// conta e troca o espelho. É um full sync da coleção `regular` do app state.
// Exige a trava da sessão.
func (s *Session) syncLabelsLocked(ctx context.Context, ls *labelSync) (err error) {
	cli, err := s.labelsClient()
	if err != nil {
		return err
	}
	ls.lastAttempt = time.Now()
	fetchCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), labelSyncTimeout)
	defer cancel()
	// gravações do espelho com prazo próprio: o full sync pode ter gasto o dele
	dbCtx, cancelDB := context.WithTimeout(context.WithoutCancel(ctx), labelSendTimeout)
	defer cancelDB()

	ls.begin()
	defer func() {
		ls.lastErr = err
		if err != nil {
			ls.drain(true)
			// Uma leitura que falhou pode ter aplicado patches sem emitir os
			// eventos; o espelho deixa de ser confiável até a próxima dar certo.
			if e := s.mgr.store.markLabelsUnsynced(dbCtx, s.id); e != nil {
				s.log.Warn("etiqueta: falha ao invalidar o espelho", "err", e)
			}
		}
	}()

	// O whatsmeow só emite os eventos de um full sync com este flag ligado. Fica
	// ligado só durante este sync, para não mudar o pareamento das sessões.
	prev := cli.EmitAppStateEventsOnFullSync
	cli.EmitAppStateEventsOnFullSync = true
	func() {
		defer func() { cli.EmitAppStateEventsOnFullSync = prev }()
		err = cli.FetchAppState(fetchCtx, appstate.WAPatchRegular, true, false)
	}()
	if err != nil {
		return err
	}
	labels, chatMap := ls.drain(false)
	chats := labeledChats(chatMap)
	if err = labelStoreErr(s.mgr.store.replaceLabels(dbCtx, s.id, labels, chats)); err != nil {
		return err
	}
	// O que chegou enquanto o espelho era trocado entra por cima, na ordem.
	lateLabels, lateChats := ls.drain(true)
	for _, l := range lateLabels {
		if e := s.mgr.store.upsertLabel(dbCtx, s.id, l); e != nil {
			s.log.Warn("etiqueta: falha ao gravar", "label", l.ID, "err", e)
		}
	}
	for c, on := range lateChats {
		if e := s.mgr.store.setChatLabel(dbCtx, s.id, c.LabelID, c.ChatJID, on); e != nil {
			s.log.Warn("etiqueta: falha ao gravar conversa", "label", c.LabelID, "err", e)
		}
	}
	s.log.Info("etiquetas sincronizadas", "labels", len(labels), "chats", len(chats))
	return nil
}

// ensureLabelsLocked garante um espelho válido, relendo do WhatsApp se ele
// nunca foi lido ou venceu. Exige a trava da sessão.
func (s *Session) ensureLabelsLocked(ctx context.Context, ls *labelSync) error {
	at, err := s.mgr.store.labelsSyncedAt(ctx, s.id)
	if err != nil {
		return labelStoreErr(err)
	}
	if labelMirrorFresh(at) {
		return nil
	}
	if ls.lastErr != nil && time.Since(ls.lastAttempt) < labelSyncBackoff {
		if at > 0 {
			return nil // espelho vencido mas legível: serve enquanto espera
		}
		return fmt.Errorf("%w (%v)", errLabelSyncWait, ls.lastErr)
	}
	err = s.syncLabelsLocked(ctx, ls)
	if err != nil && at > 0 && errors.Is(err, errLabelsOffline) {
		return nil // sessão fora do ar: o espelho que existe ainda serve para leitura
	}
	return err
}

// ensureLabelsSynced é a versão para leituras: só pega a trava se o espelho
// precisar ser relido.
func (s *Session) ensureLabelsSynced(ctx context.Context) error {
	at, err := s.mgr.store.labelsSyncedAt(ctx, s.id)
	if err != nil {
		return labelStoreErr(err)
	}
	if labelMirrorFresh(at) {
		return nil
	}
	ls := s.lockLabels()
	defer ls.run.Unlock()
	return s.ensureLabelsLocked(ctx, ls)
}

// forceLabelSync relê tudo a pedido, respeitando o intervalo mínimo e a espera
// depois de uma falha.
func (s *Session) forceLabelSync(ctx context.Context) error {
	ls := s.lockLabels()
	defer ls.run.Unlock()
	if since := time.Since(ls.lastAttempt); !ls.lastAttempt.IsZero() {
		if ls.lastErr == nil && since < labelSyncMinInterval {
			return nil
		}
		if ls.lastErr != nil && since < labelSyncBackoff {
			return fmt.Errorf("%w (%v)", errLabelSyncWait, ls.lastErr)
		}
	}
	return s.syncLabelsLocked(ctx, ls)
}

// writeLabels roda uma escrita de etiqueta com a sessão travada, o espelho
// válido e a coleção `regular` com versão local (sem ela o patch sairia contra
// um estado vazio).
func (s *Session) writeLabels(ctx context.Context, fn func(ctx context.Context, cli *whatsmeow.Client) error) error {
	cli, err := s.labelsClient()
	if err != nil {
		return err
	}
	ls := s.lockLabels()
	defer ls.run.Unlock()
	if err := s.ensureLabelsLocked(ctx, ls); err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), labelSendTimeout)
	defer cancel()
	version, _, err := cli.Store.AppState.GetAppStateVersion(ctx, string(appstate.WAPatchRegular))
	if err != nil {
		return err
	}
	if version == 0 {
		return errLabelNotReady
	}
	return fn(ctx, cli)
}

// createLabel cria a etiqueta; se já existe uma viva com o mesmo nome, devolve
// ela (created=false) em vez de duplicar.
func (s *Session) createLabel(ctx context.Context, name string, color int32) (l waLabel, created bool, err error) {
	err = s.writeLabels(ctx, func(ctx context.Context, cli *whatsmeow.Client) error {
		known, err := s.mgr.store.listLabels(ctx, s.id)
		if err != nil {
			return labelStoreErr(err)
		}
		if found, ok := findLabelByName(known, name); ok {
			l = found
			return nil
		}
		if len(userLabels(known)) >= labelMaxPerAccount {
			return errLabelLimit
		}
		l = newLabel(nextLabelID(known), newLabelAction(name, color, known))
		if err := cli.SendAppState(ctx, buildLabelEditPatch(l.ID, l.act)); err != nil {
			return err
		}
		created = true
		return labelStoreErr(s.mgr.store.upsertLabel(ctx, s.id, l))
	})
	return
}

// editLabel renomeia/recolore (name/color não nulos) ou apaga a etiqueta.
func (s *Session) editLabel(ctx context.Context, id string, name *string, color *int32, del bool) (l waLabel, err error) {
	err = s.writeLabels(ctx, func(ctx context.Context, cli *whatsmeow.Client) error {
		var err error
		if l, err = s.mgr.store.getLabel(ctx, s.id, id); err != nil {
			return err
		}
		if !l.isUserLabel() || l.Immutable {
			return errLabelSystem
		}
		if name != nil {
			l.Name = *name
		}
		if color != nil {
			l.Color = *color
		}
		l.Deleted = del
		l.act = editedLabelAction(l)
		if err := cli.SendAppState(ctx, buildLabelEditPatch(l.ID, l.act)); err != nil {
			return err
		}
		return labelStoreErr(s.mgr.store.upsertLabel(ctx, s.id, l))
	})
	return
}

// labelChatTarget resolve o JID sob o qual a conversa é etiquetada.
func (s *Session) labelChatTarget(ctx context.Context, cli *whatsmeow.Client, chat types.JID, asked string) (types.JID, error) {
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
		return types.JID{}, labelStoreErr(err)
	}
	want := pickLabelAddressing(asked, lidSeen, pnSeen, cli.Store.LIDMigrationTimestamp > 0)
	have := "pn"
	if chat.Server == types.HiddenUserServer {
		have = "lid"
	}
	if want == have {
		return chat, nil
	}
	// troca de forma só quando o par telefone/LID é conhecido; senão fica o pedido
	if want == "lid" {
		if lid, err := cli.Store.LIDs.GetLIDForPN(ctx, chat); err == nil && lid.User != "" {
			return lid.ToNonAD(), nil
		}
	} else if pn, err := cli.Store.LIDs.GetPNForLID(ctx, chat); err == nil && pn.User != "" {
		return pn.ToNonAD(), nil
	}
	return chat, nil
}

// setChatLabel aplica (ou tira) a etiqueta da conversa e devolve os JIDs usados.
//
// A mesma conversa pode estar etiquetada sob o telefone ou sob o LID. Para
// aplicar, vale a forma que já está no espelho (ou a regra de endereço, se a
// conversa ainda não tem a etiqueta). Para tirar, o patch cobre TODAS as formas
// etiquetadas: tirar só de uma deixaria a etiqueta no aparelho.
func (s *Session) setChatLabel(ctx context.Context, chat types.JID, labelID, asked string, labeled bool) (used []string, err error) {
	if chat.Server != types.GroupServer && chat.Server != types.DefaultUserServer && chat.Server != types.HiddenUserServer {
		return nil, errLabelBadTarget
	}
	err = s.writeLabels(ctx, func(ctx context.Context, cli *whatsmeow.Client) error {
		l, err := s.mgr.store.getLabel(ctx, s.id, labelID)
		if err != nil {
			return err
		}
		if !l.isUserLabel() {
			return errLabelSystem
		}
		current, err := s.mgr.store.labeledAliases(ctx, s.id, labelID, s.chatAliases(chat.ToNonAD()))
		if err != nil {
			return labelStoreErr(err)
		}
		var targets []types.JID
		for _, a := range current {
			if j, err := types.ParseJID(a); err == nil {
				targets = append(targets, j)
			}
		}
		if len(targets) == 0 || (asked != "" && labeled) {
			t, err := s.labelChatTarget(ctx, cli, chat, asked)
			if err != nil {
				return err
			}
			if labeled {
				targets = []types.JID{t}
			} else {
				targets = append(targets, t)
			}
		} else if labeled {
			targets = targets[:1]
		}
		if err := cli.SendAppState(ctx, buildLabelChatsPatch(labelID, targets, labeled)); err != nil {
			return err
		}
		for _, t := range targets {
			used = append(used, t.String())
			if err := s.mgr.store.setChatLabel(ctx, s.id, labelID, t.String(), labeled); err != nil {
				return labelStoreErr(err)
			}
		}
		return nil
	})
	return
}

// ---------- HTTP ----------

func writeLabelError(w http.ResponseWriter, err error) {
	code := http.StatusBadGateway // falha vinda do WhatsApp
	switch {
	case errors.Is(err, errLabelNotFound):
		code = http.StatusNotFound
	case errors.Is(err, errLabelsOffline), errors.Is(err, errLabelLimit), errors.Is(err, errLabelSystem):
		code = http.StatusConflict
	case errors.Is(err, errLabelName), errors.Is(err, errLabelColor), errors.Is(err, errLabelBadTarget), errors.Is(err, errLabelAddressing):
		code = http.StatusBadRequest
	case errors.Is(err, whatsmeow.ErrAppStateRateLimited):
		code = http.StatusTooManyRequests
		w.Header().Set("Retry-After", "60")
	case errors.Is(err, errLabelSyncWait), errors.Is(err, errLabelNotReady):
		code = http.StatusServiceUnavailable
		w.Header().Set("Retry-After", strconv.Itoa(int(labelSyncBackoff.Seconds())))
	case errors.Is(err, errLabelStore):
		code = http.StatusInternalServerError
	}
	writeJSON(w, code, map[string]string{"error": err.Error()})
}

type labelsResponse struct {
	SyncedAt int64     `json:"syncedAt"` // ms; quando o espelho foi lido por inteiro do WhatsApp
	Labels   []waLabel `json:"labels"`
}

// writeLabelList responde a lista. Por padrão só as etiquetas de usuário vivas;
// ?all=true traz também as apagadas e as listas do WhatsApp; ?raw=true inclui o
// registro completo de cada uma no app state.
func (s *server) writeLabelList(w http.ResponseWriter, r *http.Request, sess *Session) {
	labels, err := s.sessions.store.listLabels(r.Context(), sess.id)
	if err != nil {
		writeLabelError(w, labelStoreErr(err))
		return
	}
	if r.URL.Query().Get("all") != "true" {
		labels = userLabels(labels)
	}
	if r.URL.Query().Get("raw") == "true" {
		for i := range labels {
			if labels[i].act != nil {
				if b, err := protojson.Marshal(labels[i].act); err == nil {
					labels[i].Action = b
				}
			}
		}
	}
	at, _ := s.sessions.store.labelsSyncedAt(r.Context(), sess.id)
	writeJSON(w, http.StatusOK, labelsResponse{SyncedAt: at, Labels: labels})
}

// GET /api/sessions/{sid}/labels?all=&raw=  → etiquetas da conta
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
	s.writeLabelList(w, r, sess)
}

// POST /api/sessions/{sid}/labels/sync  → relê tudo do WhatsApp e devolve a lista
func (s *server) handleSyncLabels(w http.ResponseWriter, r *http.Request) {
	sess := s.sessionByID(w, r.PathValue("sid"))
	if sess == nil {
		return
	}
	if err := sess.forceLabelSync(r.Context()); err != nil {
		writeLabelError(w, err)
		return
	}
	s.writeLabelList(w, r, sess)
}

type labelBody struct {
	Name  *string `json:"name"`
	Color *int32  `json:"color"`
}

func readLabelBody(w http.ResponseWriter, r *http.Request) (labelBody, error) {
	var b labelBody
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, labelBodyMaxBytes)).Decode(&b); err != nil {
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
	b, err := readLabelBody(w, r)
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
	b, err := readLabelBody(w, r)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	l, err := sess.editLabel(r.Context(), r.PathValue("labelId"), b.Name, b.Color, false)
	if err != nil {
		writeLabelError(w, err)
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
	l, err := sess.editLabel(r.Context(), r.PathValue("labelId"), nil, nil, true)
	if err != nil {
		writeLabelError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"id": l.ID, "deleted": true})
}

// GET /api/sessions/{sid}/labels/{labelId}/chats?limit=&offset=  → conversas com a etiqueta
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
	limit, offset := queryPage(r, 200)
	jids, err := s.sessions.store.chatsOfLabel(r.Context(), sess.id, l.ID, limit, offset)
	if err != nil {
		writeLabelError(w, labelStoreErr(err))
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
		writeLabelError(w, labelStoreErr(err))
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
		addressing := r.URL.Query().Get("addressing")
		if !validLabelAddressing(addressing) {
			writeLabelError(w, errLabelAddressing)
			return
		}
		labelID := r.PathValue("labelId")
		used, err := sess.setChatLabel(r.Context(), chat, labelID, addressing, labeled)
		if err != nil {
			writeLabelError(w, err)
			return
		}
		first := ""
		if len(used) > 0 {
			first = used[0]
		}
		writeJSON(w, http.StatusOK, map[string]any{"chat": first, "chats": used, "labelId": labelID, "labeled": labeled})
	}
}
