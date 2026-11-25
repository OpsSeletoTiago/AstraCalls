package main

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/appstate"
	"go.mau.fi/whatsmeow/proto/waSyncAction"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
	"google.golang.org/protobuf/proto"
)

func listType(t waSyncAction.LabelEditAction_ListType) *waSyncAction.LabelEditAction_ListType {
	return &t
}

func TestNextLabelID(t *testing.T) {
	if got := nextLabelID(nil); got != "1" {
		t.Fatalf("conta sem etiqueta começa no 1, veio %q", got)
	}
	known := []waLabel{{ID: "1"}, {ID: "5"}, {ID: "12", Deleted: true}, {ID: "abc"}}
	// a apagada também conta: id nunca é reaproveitado
	if got := nextLabelID(known); got != "13" {
		t.Fatalf("próximo id deveria ser 13, veio %q", got)
	}
}

func TestSortLabelsNumeric(t *testing.T) {
	ls := []waLabel{{ID: "10"}, {ID: "abc"}, {ID: "2"}, {ID: "1"}}
	sortLabels(ls)
	got := ""
	for _, l := range ls {
		got += l.ID + ","
	}
	if got != "1,2,10,abc," {
		t.Fatalf("ordem numérica esperada, veio %s", got)
	}
}

func TestCleanLabelName(t *testing.T) {
	if n, err := cleanLabelName("  SDR RAILA  "); err != nil || n != "SDR RAILA" {
		t.Fatalf("nome deveria sair aparado: %q %v", n, err)
	}
	if _, err := cleanLabelName("   "); err == nil {
		t.Fatal("nome vazio deveria falhar")
	}
	long := make([]rune, labelMaxNameRunes+1)
	for i := range long {
		long[i] = 'ç' // conta por caractere, não por byte
	}
	if _, err := cleanLabelName(string(long)); err == nil {
		t.Fatal("nome acima do limite deveria falhar")
	}
	if _, err := cleanLabelName(string(long[:labelMaxNameRunes])); err != nil {
		t.Fatalf("nome no limite deveria passar: %v", err)
	}
}

func TestValidLabelColorAndAddressing(t *testing.T) {
	for _, c := range []int32{0, 7, 19} {
		if !validLabelColor(c) {
			t.Fatalf("cor %d deveria valer", c)
		}
	}
	for _, c := range []int32{-1, 20} {
		if validLabelColor(c) {
			t.Fatalf("cor %d não deveria valer", c)
		}
	}
	for _, a := range []string{"", "pn", "lid"} {
		if !validLabelAddressing(a) {
			t.Fatalf("addressing %q deveria valer", a)
		}
	}
	if validLabelAddressing("telefone") {
		t.Fatal("addressing desconhecido tem de ser recusado, não ignorado")
	}
}

// O índice de etiquetas guarda também as listas do WhatsApp. Só etiqueta de
// usuário entra na listagem padrão, na contagem do limite e na busca por nome.
func TestUserLabelsLeaveSystemListsOut(t *testing.T) {
	ls := []waLabel{
		newLabel("1", &waSyncAction.LabelEditAction{Name: proto.String("Novo cliente"), PredefinedID: proto.Int32(1), Type: listType(waSyncAction.LabelEditAction_PREDEFINED)}),
		newLabel("2", &waSyncAction.LabelEditAction{Name: proto.String("SDR Raila"), Deleted: proto.Bool(true), Type: listType(waSyncAction.LabelEditAction_CUSTOM)}),
		newLabel("7", &waSyncAction.LabelEditAction{Name: proto.String(" sdr raila "), Type: listType(waSyncAction.LabelEditAction_CUSTOM)}),
		newLabel("8", &waSyncAction.LabelEditAction{Name: proto.String("FB300")}), // registro antigo, sem tipo
		newLabel("9", &waSyncAction.LabelEditAction{Name: proto.String("Favoritos"), Type: listType(waSyncAction.LabelEditAction_FAVORITES)}),
		{ID: "10", Name: "sem registro"}, // veio de versão que não guardava o registro
	}
	got := ""
	for _, l := range userLabels(ls) {
		got += l.ID + ","
	}
	if got != "1,7,8,10," {
		t.Fatalf("esperado só etiqueta de usuário viva (1,7,8,10), veio %s", got)
	}
	if ls[4].isUserLabel() || ls[4].Type != "favorites" {
		t.Fatalf("lista do WhatsApp não é etiqueta de usuário: %+v", ls[4])
	}
	l, ok := findLabelByName(ls, "SDR RAILA")
	if !ok || l.ID != "7" {
		t.Fatalf("deveria achar a viva (7), ignorando a apagada e a caixa: %+v %v", l, ok)
	}
	if _, ok := findLabelByName(ls, "favoritos"); ok {
		t.Fatal("lista do WhatsApp não pode ser devolvida como etiqueta existente")
	}
}

func TestPickLabelAddressing(t *testing.T) {
	cases := []struct {
		name            string
		asked           string
		lidSeen, pnSeen int
		migrated        bool
		want            string
	}{
		{"pedido explícito vence tudo", "pn", 50, 0, true, "pn"},
		{"pedido explícito lid", "lid", 0, 50, false, "lid"},
		{"segue o que o aparelho gravou (lid)", "", 12, 1, false, "lid"},
		{"segue o que o aparelho gravou (pn)", "", 0, 4, true, "pn"},
		{"sem histórico, conta migrada usa lid", "", 0, 0, true, "lid"},
		{"sem histórico, conta não migrada usa telefone", "", 0, 0, false, "pn"},
		{"empate cai na migração", "", 3, 3, true, "lid"},
	}
	for _, c := range cases {
		if got := pickLabelAddressing(c.asked, c.lidSeen, c.pnSeen, c.migrated); got != c.want {
			t.Errorf("%s: esperado %s, veio %s", c.name, c.want, got)
		}
	}
}

func TestLabelFromEdit(t *testing.T) {
	l := labelFromEdit(&events.LabelEdit{LabelID: "9", Action: &waSyncAction.LabelEditAction{
		Name: proto.String("SDR THIAGO"), Color: proto.Int32(4), PredefinedID: proto.Int32(2), Deleted: proto.Bool(true),
		IsImmutable: proto.Bool(true), Type: listType(waSyncAction.LabelEditAction_CUSTOM),
	}})
	if l.ID != "9" || l.Name != "SDR THIAGO" || l.Color != 4 || l.PredefinedID != 2 || !l.Deleted || !l.Immutable || l.Type != "custom" {
		t.Fatalf("mapeamento errado: %+v", l)
	}
	// mutação sem Action não pode dar panic
	if l := labelFromEdit(&events.LabelEdit{LabelID: "3"}); l.ID != "3" || l.Name != "" || l.Deleted || !l.isUserLabel() {
		t.Fatalf("evento sem action deveria sair zerado: %+v", l)
	}
}

// Um SET troca o valor inteiro do índice: renomear não pode apagar os campos
// que a API não conhece (ordem, ativo, tipo, predefinedID).
func TestEditedLabelActionPreservesRecord(t *testing.T) {
	orig := &waSyncAction.LabelEditAction{
		Name: proto.String("SDR RAILA"), Color: proto.Int32(2), Deleted: proto.Bool(false),
		OrderIndex: proto.Int32(7), IsActive: proto.Bool(true), PredefinedID: proto.Int32(3),
		Type: listType(waSyncAction.LabelEditAction_CUSTOM),
	}
	l := newLabel("6", orig)
	l.Name, l.Color = "SDR THIAGO", 9
	act := editedLabelAction(l)
	if act.GetName() != "SDR THIAGO" || act.GetColor() != 9 || act.GetDeleted() {
		t.Fatalf("nome/cor novos não entraram: %+v", act)
	}
	if act.GetOrderIndex() != 7 || !act.GetIsActive() || act.GetPredefinedID() != 3 || act.GetType() != waSyncAction.LabelEditAction_CUSTOM {
		t.Fatalf("campos do registro deveriam ser preservados: %+v", act)
	}
	if orig.GetName() != "SDR RAILA" {
		t.Fatal("o registro original não pode ser alterado (a edição parte de uma cópia)")
	}
	// apagar também parte do registro
	l.Deleted = true
	if act := editedLabelAction(l); !act.GetDeleted() || act.GetOrderIndex() != 7 {
		t.Fatalf("apagar deveria manter o registro e marcar deleted: %+v", act)
	}
	// etiqueta sem registro guardado: sai o essencial, com o predefinedID que houver
	act = editedLabelAction(waLabel{ID: "1", Name: "Novo cliente", PredefinedID: 1})
	if act.GetName() != "Novo cliente" || act.GetPredefinedID() != 1 || act.Type != nil {
		t.Fatalf("registro mínimo errado: %+v", act)
	}
}

func TestNewLabelActionFollowsDeviceMold(t *testing.T) {
	// conta sem etiqueta criada no aparelho: só o essencial
	act := newLabelAction("FB300", 3, nil)
	if act.GetName() != "FB300" || act.GetColor() != 3 || act.GetDeleted() || act.Type != nil || act.OrderIndex != nil || act.IsActive != nil {
		t.Fatalf("sem molde deveria sair só nome/cor/deleted: %+v", act)
	}
	// com etiquetas do aparelho: a nova segue o tipo/ativo delas e entra no fim da ordem
	known := []waLabel{
		newLabel("1", &waSyncAction.LabelEditAction{Name: proto.String("Novo cliente"), OrderIndex: proto.Int32(9), Type: listType(waSyncAction.LabelEditAction_PREDEFINED)}),
		newLabel("6", &waSyncAction.LabelEditAction{Name: proto.String("SDR RAILA"), OrderIndex: proto.Int32(4), IsActive: proto.Bool(true), Type: listType(waSyncAction.LabelEditAction_CUSTOM)}),
		newLabel("7", &waSyncAction.LabelEditAction{Name: proto.String("velha"), Deleted: proto.Bool(true), Type: listType(waSyncAction.LabelEditAction_CUSTOM)}),
	}
	act = newLabelAction("SDR THIAGO", 1, known)
	if act.GetType() != waSyncAction.LabelEditAction_CUSTOM || !act.GetIsActive() || act.GetOrderIndex() != 10 {
		t.Fatalf("deveria seguir o molde (custom, ativa, ordem 10): %+v", act)
	}
}

func TestBuildLabelPatches(t *testing.T) {
	act := &waSyncAction.LabelEditAction{Name: proto.String("FB300"), Color: proto.Int32(3), Deleted: proto.Bool(false)}
	p := buildLabelEditPatch("6", act)
	if p.Type != appstate.WAPatchRegular || len(p.Mutations) != 1 {
		t.Fatalf("patch deveria ser da coleção regular com 1 mutação: %+v", p)
	}
	m := p.Mutations[0]
	if len(m.Index) != 2 || m.Index[0] != appstate.IndexLabelEdit || m.Index[1] != "6" || m.Version != 3 || m.Value.GetLabelEditAction() != act {
		t.Fatalf("índice/versão/valor errados: %+v", m)
	}
	// mesmo índice e versão do patch que o whatsmeow monta
	ref := appstate.BuildLabelEdit("6", "FB300", 3, false).Mutations[0]
	if ref.Index[0] != m.Index[0] || ref.Index[1] != m.Index[1] || ref.Version != m.Version {
		t.Fatalf("patch divergiu do whatsmeow: %+v x %+v", ref, m)
	}

	pn := types.NewJID("5547999990000", types.DefaultUserServer)
	lid := types.NewJID("123456789", types.HiddenUserServer)
	cp := buildLabelChatsPatch("6", []types.JID{pn, lid}, false)
	if cp.Type != appstate.WAPatchRegular || len(cp.Mutations) != 2 {
		t.Fatalf("tirar das duas formas deveria ser um patch com 2 mutações: %+v", cp)
	}
	for i, j := range []types.JID{pn, lid} {
		mu := cp.Mutations[i]
		if mu.Index[0] != appstate.IndexLabelAssociationChat || mu.Index[1] != "6" || mu.Index[2] != j.String() || mu.Value.GetLabelAssociationAction().GetLabeled() {
			t.Fatalf("mutação %d errada: %+v", i, mu)
		}
	}
}

func TestLabelSyncCollectsLastValue(t *testing.T) {
	ls := &labelSync{}
	chat := labelChat{LabelID: "1", ChatJID: "5511@s.whatsapp.net"}
	if ls.addLabel(waLabel{ID: "1"}) || ls.addChat(chat, true) {
		t.Fatal("fora de um full sync nada é recolhido (o evento vai direto ao banco)")
	}
	ls.begin()
	// snapshot diz "etiquetada"; um patch seguinte desfaz. Vale o último.
	ls.addLabel(waLabel{ID: "1", Name: "antigo"})
	ls.addLabel(waLabel{ID: "1", Name: "novo"})
	ls.addChat(chat, true)
	ls.addChat(chat, false)
	ls.addChat(labelChat{LabelID: "1", ChatJID: "999@lid"}, true)
	labels, chats := ls.drain(false)
	if len(labels) != 1 || labels[0].Name != "novo" {
		t.Fatalf("deveria ficar o último valor da etiqueta: %+v", labels)
	}
	on := labeledChats(chats)
	if len(on) != 1 || on[0].ChatJID != "999@lid" {
		t.Fatalf("a conversa desetiquetada depois não pode voltar: %+v", on)
	}
	// drain sem stop continua recolhendo (o que chega durante a troca do espelho)
	if !ls.addLabel(waLabel{ID: "2"}) {
		t.Fatal("depois do drain(false) a coleta continua")
	}
	late, _ := ls.drain(true)
	if len(late) != 1 || late[0].ID != "2" {
		t.Fatalf("o que chegou depois deveria sair no drain final: %+v", late)
	}
	if ls.addLabel(waLabel{ID: "3"}) {
		t.Fatal("depois do drain(true) a coleta termina")
	}
}

func TestLabelMirrorFresh(t *testing.T) {
	old := labelMirrorMaxAge
	defer func() { labelMirrorMaxAge = old }()
	labelMirrorMaxAge = 24 * time.Hour
	if labelMirrorFresh(0) {
		t.Fatal("espelho nunca lido não é válido")
	}
	if !labelMirrorFresh(time.Now().Add(-time.Hour).UnixMilli()) {
		t.Fatal("espelho de 1 h atrás ainda vale")
	}
	if labelMirrorFresh(time.Now().Add(-25 * time.Hour).UnixMilli()) {
		t.Fatal("espelho de 25 h atrás venceu")
	}
	labelMirrorMaxAge = 0 // releitura automática desligada
	if !labelMirrorFresh(time.Now().Add(-1000 * time.Hour).UnixMilli()) {
		t.Fatal("com a validade desligada, espelho lido uma vez vale sempre")
	}
}

func TestLabelWebhookData(t *testing.T) {
	d := chatLabelWebhookData(types.NewJID("5547999990000", types.DefaultUserServer), "5547999990000", "7", "SDR RAILA", true)
	if d["chat"] != "5547999990000@s.whatsapp.net" || d["labelId"] != "7" || d["labelName"] != "SDR RAILA" || d["labeled"] != true || d["isGroup"] != false || d["phone"] != "5547999990000" {
		t.Fatalf("payload errado: %+v", d)
	}
	// conversa por LID sem telefone conhecido: o campo phone não sai (nunca os dígitos do LID)
	d = chatLabelWebhookData(types.NewJID("123456789", types.HiddenUserServer), "", "7", "", false)
	if _, has := d["phone"]; has || d["chat"] != "123456789@lid" {
		t.Fatalf("LID sem telefone não deveria trazer phone: %+v", d)
	}
	// grupo: isGroup verdadeiro, para o filtro ignoreGroups do webhook valer
	d = chatLabelWebhookData(types.NewJID("120363000000000000", types.GroupServer), "", "2", "FB300", true)
	if d["isGroup"] != true || !isGroupWebhookEvent("chat_label", d) {
		t.Fatalf("etiqueta em grupo deveria contar como evento de grupo: %+v", d)
	}
	l := labelWebhookData(newLabel("4", &waSyncAction.LabelEditAction{Name: proto.String("FB300"), Color: proto.Int32(2), Deleted: proto.Bool(true), Type: listType(waSyncAction.LabelEditAction_CUSTOM)}))
	if l["id"] != "4" || l["name"] != "FB300" || l["deleted"] != true || l["type"] != "custom" {
		t.Fatalf("payload de etiqueta errado: %+v", l)
	}
}

func TestWriteLabelErrorStatus(t *testing.T) {
	cases := []struct {
		err        error
		code       int
		retryAfter bool
	}{
		{errLabelNotFound, http.StatusNotFound, false},
		{errLabelsOffline, http.StatusConflict, false},
		{errLabelLimit, http.StatusConflict, false},
		{errLabelSystem, http.StatusConflict, false},
		{errLabelName, http.StatusBadRequest, false},
		{errLabelAddressing, http.StatusBadRequest, false},
		{whatsmeow.ErrAppStateRateLimited, http.StatusTooManyRequests, true},
		{errLabelSyncWait, http.StatusServiceUnavailable, true},
		{errLabelNotReady, http.StatusServiceUnavailable, true},
		{labelStoreErr(errors.New("conexão caiu")), http.StatusInternalServerError, false},
		{errors.New("erro do WhatsApp"), http.StatusBadGateway, false},
	}
	for _, c := range cases {
		w := httptest.NewRecorder()
		writeLabelError(w, c.err)
		if w.Code != c.code {
			t.Errorf("%v: esperado %d, veio %d", c.err, c.code, w.Code)
		}
		if got := w.Header().Get("Retry-After") != ""; got != c.retryAfter {
			t.Errorf("%v: Retry-After presente=%v, esperado %v", c.err, got, c.retryAfter)
		}
	}
	if labelStoreErr(nil) != nil {
		t.Fatal("sem erro de banco, sem erro")
	}
}

// Evento de etiqueta numa sessão sem store (testes, sessão ainda montando) não
// pode derrubar o tratamento dos outros eventos; e evento que não é de etiqueta
// passa direto.
func TestHandleLabelEventWithoutStore(t *testing.T) {
	s := &Session{}
	s.handleLabelEvent(&events.LabelEdit{LabelID: "1"})
	s.handleLabelEvent(&events.LabelAssociationChat{LabelID: "1"})
	s.handleLabelEvent(&events.LoggedOut{})
	s.handleLabelEvent(&events.PairSuccess{})
	s.handleLabelEvent(&events.Message{})
}

// As rotas de etiqueta exigem a chave-mestra: a chave de widget só abre as
// rotas de chamada, mesmo que o chatId traga "/calls" escapado.
func TestLabelRoutesNeedMasterKey(t *testing.T) {
	paths := []struct{ method, path string }{
		{"GET", "/api/sessions/abc/labels"},
		{"POST", "/api/sessions/abc/labels/sync"},
		{"PUT", "/api/sessions/abc/chats/5547999990000/labels/1"},
		{"PUT", "/api/sessions/abc/chats/5547999990000%2Fcalls/labels/1"},
		{"GET", "/api/sessions/abc/chats/5547999990000%2Fcalls/labels"},
		{"GET", "/api/sessions/abc/chats/5547999990000%2Fcalls/messages"},
		{"DELETE", "/api/sessions/abc/labels/%2Fcalls"},
	}
	for _, c := range paths {
		if widgetAllowed(httptest.NewRequest(c.method, c.path, nil)) {
			t.Errorf("chave de widget NÃO deveria abrir %s %s", c.method, c.path)
		}
	}
}
