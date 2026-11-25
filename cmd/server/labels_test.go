package main

import (
	"testing"

	"go.mau.fi/whatsmeow/appstate"
	"go.mau.fi/whatsmeow/proto/waSyncAction"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
	"google.golang.org/protobuf/proto"
)

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

func TestValidLabelColor(t *testing.T) {
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
}

func TestFindLabelByName(t *testing.T) {
	ls := []waLabel{{ID: "1", Name: "SDR Raila", Deleted: true}, {ID: "7", Name: " sdr raila "}, {ID: "8", Name: "FB300"}}
	l, ok := findLabelByName(ls, "SDR RAILA")
	if !ok || l.ID != "7" {
		t.Fatalf("deveria achar a viva (7), ignorando a apagada e a caixa: %+v %v", l, ok)
	}
	if _, ok := findLabelByName(ls, "SDR THIAGO"); ok {
		t.Fatal("não existe etiqueta com esse nome")
	}
	if n := countLiveLabels(ls); n != 2 {
		t.Fatalf("2 etiquetas vivas, veio %d", n)
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
		{"pedido desconhecido é ignorado", "outro", 0, 2, true, "pn"},
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
	}})
	if l.ID != "9" || l.Name != "SDR THIAGO" || l.Color != 4 || l.PredefinedID != 2 || !l.Deleted {
		t.Fatalf("mapeamento errado: %+v", l)
	}
	// mutação de remoção chega sem Action: não pode dar panic
	if l := labelFromEdit(&events.LabelEdit{LabelID: "3"}); l.ID != "3" || l.Name != "" || l.Deleted {
		t.Fatalf("evento sem action deveria sair zerado: %+v", l)
	}
}

func TestBuildLabelEditPatch(t *testing.T) {
	p := buildLabelEditPatch(waLabel{ID: "6", Name: "FB300", Color: 3})
	if p.Type != appstate.WAPatchRegular || len(p.Mutations) != 1 {
		t.Fatalf("patch deveria ser da coleção regular com 1 mutação: %+v", p)
	}
	m := p.Mutations[0]
	if len(m.Index) != 2 || m.Index[0] != appstate.IndexLabelEdit || m.Index[1] != "6" || m.Version != 3 {
		t.Fatalf("índice/versão errados: %+v", m)
	}
	a := m.Value.GetLabelEditAction()
	if a.GetName() != "FB300" || a.GetColor() != 3 || a.GetDeleted() || a.PredefinedID != nil {
		t.Fatalf("ação errada: %+v", a)
	}
	// etiqueta de fábrica mantém o predefinedID ao ser editada/apagada
	a = buildLabelEditPatch(waLabel{ID: "1", Name: "Novo cliente", PredefinedID: 1, Deleted: true}).Mutations[0].Value.GetLabelEditAction()
	if a.GetPredefinedID() != 1 || !a.GetDeleted() {
		t.Fatalf("predefinedID/deleted deveriam seguir no patch: %+v", a)
	}
}

func TestLabelSyncCollects(t *testing.T) {
	ls := &labelSync{}
	if ls.addLabel(waLabel{ID: "1"}) || ls.addChat(labelChat{LabelID: "1", ChatJID: "x"}) {
		t.Fatal("fora de um full sync nada é recolhido (o evento vai direto ao banco)")
	}
	ls.begin()
	if !ls.addLabel(waLabel{ID: "1"}) || !ls.addChat(labelChat{LabelID: "1", ChatJID: "5511@s.whatsapp.net"}) {
		t.Fatal("durante o full sync os eventos são recolhidos")
	}
	labels, chats := ls.take()
	if len(labels) != 1 || len(chats) != 1 {
		t.Fatalf("take deveria devolver o recolhido: %d %d", len(labels), len(chats))
	}
	if ls.addLabel(waLabel{ID: "2"}) {
		t.Fatal("depois do take a coleta termina")
	}
	// um novo sync começa vazio
	ls.begin()
	if l, c := ls.take(); len(l) != 0 || len(c) != 0 {
		t.Fatalf("coleta nova deveria nascer vazia: %d %d", len(l), len(c))
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
	if l := labelWebhookData(waLabel{ID: "4", Name: "FB300", Color: 2, Deleted: true}); l["id"] != "4" || l["name"] != "FB300" || l["deleted"] != true {
		t.Fatalf("payload de etiqueta errado: %+v", l)
	}
}

// Evento de etiqueta numa sessão sem store (testes, sessão ainda montando) não
// pode derrubar o tratamento dos outros eventos.
func TestHandleLabelEventWithoutStore(t *testing.T) {
	s := &Session{}
	s.handleLabelEvent(&events.LabelEdit{LabelID: "1"})
	s.handleLabelEvent(&events.LoggedOut{})
}
