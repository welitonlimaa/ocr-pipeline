# 🔒 Política de Privacidade, Uso de Dados e Conformidade Legal

Este documento descreve como os dados são tratados dentro da plataforma, com foco em transparência, segurança e conformidade com legislações como a **Lei Geral de Proteção de Dados (LGPD - Lei nº 13.709/2018)**.

---

# 1. Propriedade dos Dados

Todos os arquivos enviados e processados são de **propriedade exclusiva do usuário**.

* ❌ Não revendemos dados
* ❌ Não utilizamos para treinamento de modelos
* ❌ Não compartilhamos com terceiros

👉 A plataforma atua **apenas como processadora**, não como controladora dos dados.

---

# 2. Finalidade do Processamento

Os dados são utilizados **exclusivamente** para:

* Processamento do documento enviado (OCR e estruturação)
* Geração de outputs (chunks, índice, resumo, etc.)
* Disponibilização do resultado ao usuário

👉 Nenhuma outra finalidade é aplicada.

---

# 3. Retenção de Dados

Os dados são armazenados de forma temporária:

* **Tempo máximo de retenção: 24 horas**
* Após esse período, os dados são **automaticamente excluídos**

Isso inclui:

* arquivos originais enviados
* outputs gerados
* metadados associados

---

# 4. Exclusão de Dados

A exclusão ocorre de forma:

* automática (TTL configurado)
* irreversível
* sem necessidade de solicitação do usuário

👉 Não mantemos backups persistentes dos dados processados.

---

# 5. Segurança da Informação

Adotamos práticas modernas de segurança:

* armazenamento isolado por job
* uso de object storage seguro
* comunicação via rede privada (containers)
* controle de acesso por chave e contexto

Além disso:

* URLs de acesso podem ser temporárias (pré-assinadas)
* dados não são expostos publicamente por padrão

---

# 6. Conformidade com a LGPD

Este projeto segue os princípios da **Lei Geral de Proteção de Dados Pessoais**:

### ✔️ Finalidade

Os dados são tratados apenas para o propósito informado.

### ✔️ Necessidade

Coletamos apenas o mínimo necessário para execução do serviço.

### ✔️ Transparência

O usuário tem clareza sobre como os dados são usados.

### ✔️ Segurança

Medidas técnicas e administrativas são aplicadas para proteção.

### ✔️ Prevenção

Minimizamos riscos de vazamento ou uso indevido.

---

# 7. Papel da Plataforma

De acordo com a LGPD:

* **Usuário → Controlador dos dados**
* **Plataforma → Operadora dos dados**

👉 Ou seja, o usuário define o que será processado e para qual finalidade.

---

# 8. Responsabilidade do Usuário

O usuário é responsável por:

* garantir que possui direito de uso dos documentos enviados
* evitar envio de dados sensíveis sem necessidade
* respeitar legislações aplicáveis ao conteúdo processado

---

# 9. Dados Sensíveis

O sistema **não é projetado para armazenar dados sensíveis de forma persistente**.

Exemplos de dados sensíveis (LGPD):

* origem racial ou étnica
* convicções religiosas
* dados de saúde
* dados biométricos

👉 Caso utilizados, o processamento continua sendo temporário e descartado em até 24h.

---

# 10. Logs e Monitoramento

Logs podem ser coletados para:

* diagnóstico de erros
* melhoria da estabilidade
* auditoria técnica

No entanto:

* não armazenamos conteúdo completo dos documentos nos logs
* evitamos registrar dados pessoais

---

# 11. Arquitetura Privacy-First

O sistema foi projetado com foco em privacidade:

* processamento assíncrono e isolado
* dados efêmeros (TTL automático)
* separação por job
* ausência de banco de dados persistente para conteúdo

---

# 12. Uso com Inteligência Artificial

Caso o usuário utilize o output com ferramentas de IA:

* isso ocorre **fora da plataforma**
* sob total controle do usuário
* sem interferência ou visibilidade deste sistema

---

# 📬 13. Contato

Para dúvidas sobre privacidade, segurança ou uso de dados:

> Entre em contato com o responsável pela aplicação: welitonlimadev@gmail.com
