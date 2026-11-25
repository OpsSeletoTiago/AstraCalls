# Fork da Seleto

Este repositório é um fork do [AstraCalls](https://github.com/AstraOnlineWeb/AstraCalls)
(AGPL-3.0). Ele existe para rodar, no servidor da Seleto, a versão oficial **mais**
as alterações que ainda não entraram no projeto original. O código alterado fica
público aqui, como a licença pede.

## Como o fork é organizado

| Branch | O que tem |
|---|---|
| `feat/*` | Uma alteração por branch, limpa, pronta para virar pull request no projeto original |
| `seleto` | A versão oficial em uso + todas as `feat/*` ativas + os arquivos só nossos (este e o workflow da imagem) |

A imagem que roda no servidor sai da branch `seleto`, por uma tag `seleto-<versão oficial>-<n>`
(ex.: `seleto-v1.1.7-1`), e fica em `ghcr.io/opsseletotiago/astracalls:<tag>`.
O workflow é o [`seleto-image.yml`](.github/workflows/seleto-image.yml).

## Alterações ativas

| Alteração | Branch | Situação no projeto original |
|---|---|---|
| Etiquetas do WhatsApp Business (`/labels`, `/chats/{chatId}/labels`, eventos `label` e `chat_label`) | `feat/etiquetas-whatsapp-business` | ainda não proposta |

Quando uma alteração entra no projeto original, ela sai desta tabela e a branch
`feat/*` é apagada: a versão oficial seguinte já a traz.

## Quando sai uma versão oficial nova

1. Trazer a tag nova do projeto original.
2. Reaplicar cada `feat/*` ativa sobre ela (as alterações moram em arquivos próprios,
   com toques de uma linha nos arquivos do original, para o reencaixe ser simples).
3. Compilar e rodar os testes.
4. Refazer a branch `seleto`, criar a tag `seleto-<versão nova>-1` e esperar a imagem.
5. Trocar a imagem no servidor e conferir versão, conexões e etiquetas.

Voltar atrás é trocar a imagem do servidor de volta para a oficial
(`astraonline/astracalls:<versão>`): as tabelas de etiqueta ficam no banco sem uso.
