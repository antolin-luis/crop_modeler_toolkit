"""Generate the executive one-pager as a Word document, in EN / ES / PT.

Usage:  uv run --with python-docx python make_onepager.py [en es pt]
Writes Honduras_Climate_API_Onepager{,_ES,_PT}.docx to the toolkit repo root.
"""

import sys
from pathlib import Path

from docx import Document
from docx.enum.table import WD_TABLE_ALIGNMENT
from docx.enum.text import WD_ALIGN_PARAGRAPH
from docx.oxml import OxmlElement
from docx.oxml.ns import qn
from docx.shared import Inches, Pt, RGBColor

REPO = str(Path(__file__).resolve().parents[1])

ACCENT = RGBColor(0x0F, 0x5E, 0x7A)   # deep teal
INK = RGBColor(0x1A, 0x1A, 0x1A)
MUTED = RGBColor(0x55, 0x5F, 0x66)
RULE = "BFD3DA"
BAND = "EAF2F5"

# --- content: each rich field is a list of (text, bold) runs ----------------

T = {}

T["en"] = dict(
    out="Honduras_Climate_API_Onepager.docx",
    title="Honduras Climate Data API",
    subtitle="From a finished dataset to a sellable product in three months",
    intro="Seventy-six years of daily climate data covering all of Honduras is "
          "already acquired, processed and stored. What is missing is any way for a "
          "customer to reach it. This is the plan to close that gap — and what it "
          "costs in time and money.",
    h_stand="Where we stand",
    h_built="What gets built",
    h_time="Timeline",
    h_cost="What it costs to run",
    h_dec="Two decisions needed now — both legal, free today, expensive later",
    h_cav="One honest caveat",
    b1=[("283 grid cells", 1),
        (" cover Honduras at roughly 25 km resolution. Eight daily weather "
         "measurements per cell — temperature, rainfall, solar radiation, wind, "
         "humidity and crop water demand — for ", 0),
        ("every day since 1950", 1), (". About 8 million records.", 0)],
    b2=[("Data acquisition is ", 0), ("finished", 1),
        (": 539 files, 509 MB, already on disk. ", 0),
        ("Total spent to date: under $0.10.", 1),
        (" The source data is free from European and US public agencies.", 0)],
    b3=[("The monthly PDF report generator ", 0), ("already works today", 1),
        (". Branded, English/Spanish/Portuguese, eight pages: rainfall and "
         "temperature versus normal, drought index, growing-degree days, water "
         "balance, and per-cell maps of the country. Generated in 37 seconds from "
         "live data.", 0)],
    b4=[("What is missing is the ", 0), ("service layer", 1),
        (". Today the only access is a database password on a home network — no "
         "website, no login, nothing a customer can call. That is the gap.", 0)],
    built=[("A commercial web service. A customer sends a location — a farm, a "
            "municipality, or the whole country — plus what they want to know, and "
            "receives either data or a finished document. ", 0),
           ("60 services behind one API key", 1),
           (": daily history back to 1950; climate normals and how far the current "
            "season departs from them; drought, growing-degree and crop-water "
            "indicators; hurricane exposure; seasonal forecasts; ready-to-run DSSAT "
            "crop-model files; and ", 0),
           ("automated PDF reports", 1),
           (" — the monthly bulletin that already works, plus per-farm summaries "
            "and, later, the full government-style seasonal outlook. Access is "
            "metered by API key, the same model Stripe and OpenAI use. Payment "
            "processing is not included, but nothing has to be rebuilt to add it "
            "later.", 0)],
    assume=[("One developer working with AI assistance, ", 0),
            ("six focused hours per day, five days per week", 1),
            (", starting 1 September 2026.", 0)],
    cols=("Milestone", "Days", "Date", "What can be sold"),
    rows=[
        ("Monthly PDF report", "0", "AVAILABLE NOW",
         "Branded monthly agroclimatic bulletin, sold as a subscription today"),
        ("First live service", "20", "29 Sep 2026",
         "One secured endpoint, live on the internet"),
        ("Sellable product", "60", "24 Nov 2026",
         "History, normals, anomalies, drought indicators, crop files, bulk export"),
        ("Reports behind the API", "68", "5 Dec 2026",
         "Self-serve per-farm and per-department reports on demand"),
        ("Hazard and drought", "82", "24 Dec 2026",
         "Hurricane exposure, municipal drought alerts"),
        ("Seasonal forecasts", "103", "23 Jan 2027",
         "Forecast outlooks, subject to accuracy validation"),
        ("Full seasonal bulletin", "156", "8 Apr 2027",
         "Complete quarterly PDF product with forecasts"),
        ("Conservative finish (80% confidence)", "184", "16 May 2027", "—"),
    ],
    foot=[("Revenue can start today with the report, and at the API in under three "
           "months. ", 1),
          ("Every date above depends on sustaining six hours a day. At three hours a "
           "day the sellable API lands 16 Feb 2027 and full delivery 12 Nov 2027; at "
           "the pace this project has actually run so far (about two hours a day), "
           "sellable slips to 6 May 2027 and full delivery to June 2028. ", 0),
          ("Pace, not technical difficulty, is the variable.", 1)],
    cost=[("$18 per month", 1), (" during development · ", 0),
          ("$26 per month", 1), (" for a live business · ", 0),
          ("$51 per month", 1),
          (" at heavy load (one million requests per day). Ongoing data costs are "
           "about $0.05 per month. At these figures, ", 0),
          ("hosting is not a constraint on this project.", 1)],
    d1=[("Confirm a commercially licensed data pipeline.", 1),
        (" The fast download tool in use runs on a free non-commercial tier, which "
         "very likely does not cover selling access. Three workable alternatives "
         "exist, including one already built and running. Small cost now; a serious "
         "problem if found after launch.", 0)],
    d2=[("Change the map-boundary source.", 1),
        (" The planned source for department and municipality outlines forbids "
         "commercial redistribution. An equivalent, commercially licensed source "
         "exists. Switching before that code is written costs nothing.", 0)],
    caveat=[("Seasonal forecast accuracy over Honduras's mountainous terrain is "
             "unproven. The plan measures it before any forecast reaches a customer. "
             "If accuracy turns out to be poor, forecasts stay switched off and every "
             "other part of the product is unaffected — which is exactly why they are "
             "scheduled last, behind the parts that already earn.", 0)],
)

T["es"] = dict(
    out="Honduras_Climate_API_Onepager_ES.docx",
    title="API de Datos Climáticos de Honduras",
    subtitle="De una base de datos terminada a un producto vendible en tres meses",
    intro="Setenta y seis años de datos climáticos diarios de todo Honduras ya están "
          "descargados, procesados y almacenados. Lo que falta es una forma de que un "
          "cliente pueda acceder a ellos. Este es el plan para cerrar esa brecha, y lo "
          "que cuesta en tiempo y dinero.",
    h_stand="Dónde estamos",
    h_built="Qué se va a construir",
    h_time="Cronograma",
    h_cost="Cuánto cuesta operarlo",
    h_dec="Dos decisiones necesarias ahora — legales, gratuitas hoy, caras después",
    h_cav="Una advertencia honesta",
    b1=[("283 celdas de malla", 1),
        (" cubren Honduras a unos 25 km de resolución. Ocho mediciones diarias por "
         "celda —temperatura, lluvia, radiación solar, viento, humedad y demanda "
         "hídrica del cultivo— para ", 0),
        ("cada día desde 1950", 1), (". Unos 8 millones de registros.", 0)],
    b2=[("La adquisición de datos está ", 0), ("terminada", 1),
        (": 539 archivos, 509 MB, ya en disco. ", 0),
        ("Gasto total hasta hoy: menos de US$ 0,10.", 1),
        (" Los datos de origen son gratuitos, de organismos públicos europeos y "
         "estadounidenses.", 0)],
    b3=[("El generador mensual de informes en PDF ", 0), ("ya funciona hoy", 1),
        (". Con identidad de marca, en inglés/español/portugués, ocho páginas: lluvia "
         "y temperatura frente a lo normal, índice de sequía, grados-día de "
         "crecimiento, balance hídrico y mapas del país celda por celda. Se genera en "
         "37 segundos con datos en vivo.", 0)],
    b4=[("Lo que falta es la ", 0), ("capa de servicio", 1),
        (". Hoy el único acceso es una contraseña de base de datos en una red "
         "doméstica: sin sitio web, sin inicio de sesión, sin nada que un cliente "
         "pueda invocar. Esa es la brecha.", 0)],
    built=[("Un servicio web comercial. El cliente envía una ubicación —una finca, un "
            "municipio o el país entero— junto con lo que quiere saber, y recibe datos "
            "o un documento terminado. ", 0),
           ("60 servicios tras una sola clave de API", 1),
           (": histórico diario desde 1950; normales climáticas y cuánto se aparta la "
            "temporada actual de ellas; indicadores de sequía, grados-día y agua para "
            "el cultivo; exposición a huracanes; pronósticos estacionales; archivos "
            "listos para el modelo de cultivos DSSAT; e ", 0),
           ("informes PDF automáticos", 1),
           (" —el boletín mensual que ya funciona, más resúmenes por finca y, más "
            "adelante, la perspectiva estacional completa al estilo gubernamental. El "
            "acceso se mide por clave de API, el mismo modelo que usan Stripe y "
            "OpenAI. No se incluye el cobro, pero nada habrá que rehacer para añadirlo "
            "después.", 0)],
    assume=[("Un desarrollador trabajando con asistencia de IA, ", 0),
            ("seis horas de trabajo concentrado al día, cinco días por semana", 1),
            (", comenzando el 1 de septiembre de 2026.", 0)],
    cols=("Hito", "Días", "Fecha", "Qué se puede vender"),
    rows=[
        ("Informe mensual en PDF", "0", "DISPONIBLE YA",
         "Boletín agroclimático mensual con marca, vendible hoy por suscripción"),
        ("Primer servicio en línea", "20", "29 sep 2026",
         "Un endpoint seguro, publicado en internet"),
        ("Producto vendible", "60", "24 nov 2026",
         "Histórico, normales, anomalías, indicadores de sequía, archivos de cultivo, "
         "descargas masivas"),
        ("Informes tras la API", "68", "5 dic 2026",
         "Informes por finca y por departamento, a demanda y en autoservicio"),
        ("Amenazas y sequía", "82", "24 dic 2026",
         "Exposición a huracanes, alertas municipales de sequía"),
        ("Pronósticos estacionales", "103", "23 ene 2027",
         "Perspectivas de pronóstico, sujetas a validación de precisión"),
        ("Boletín estacional completo", "156", "8 abr 2027",
         "Producto trimestral en PDF completo, con pronósticos"),
        ("Cierre conservador (80% de confianza)", "184", "16 may 2027", "—"),
    ],
    foot=[("Los ingresos pueden empezar hoy con el informe, y con la API en menos de "
           "tres meses. ", 1),
          ("Cada fecha depende de sostener seis horas al día. A tres horas diarias la "
           "API vendible llega el 16 feb 2027 y la entrega completa el 12 nov 2027; al "
           "ritmo real que ha llevado este proyecto (unas dos horas al día), lo "
           "vendible se retrasa al 6 may 2027 y la entrega completa a junio de 2028. ",
           0),
          ("La variable es el ritmo, no la dificultad técnica.", 1)],
    cost=[("US$ 18 al mes", 1), (" durante el desarrollo · ", 0),
          ("US$ 26 al mes", 1), (" con el negocio en marcha · ", 0),
          ("US$ 51 al mes", 1),
          (" con carga alta (un millón de peticiones diarias). El coste corriente de "
           "datos ronda los US$ 0,05 al mes. Con estas cifras, ", 0),
          ("el alojamiento no es una restricción para este proyecto.", 1)],
    d1=[("Confirmar una fuente de datos con licencia comercial.", 1),
        (" La herramienta de descarga rápida que se usa funciona en un plan gratuito "
         "no comercial, que muy probablemente no cubre la venta de acceso. Existen "
         "tres alternativas viables, una de ellas ya construida y en funcionamiento. "
         "Coste pequeño ahora; problema serio si se descubre tras el lanzamiento.", 0)],
    d2=[("Cambiar la fuente de los límites cartográficos.", 1),
        (" La fuente prevista para los contornos departamentales y municipales prohíbe "
         "la redistribución comercial. Existe una fuente equivalente con licencia "
         "comercial. Cambiarla antes de escribir ese código no cuesta nada.", 0)],
    caveat=[("La precisión del pronóstico estacional sobre el terreno montañoso de "
             "Honduras no está demostrada. El plan la mide antes de que ningún "
             "pronóstico llegue a un cliente. Si resulta baja, los pronósticos quedan "
             "desactivados y el resto del producto no se ve afectado: justamente por "
             "eso están programados al final, detrás de lo que ya genera ingresos.",
             0)],
)

# Brazilian Portuguese (pt-BR): arquivos/registros/umidade/grade/fazenda/
# hospedagem/requisições/sob demanda — not pt-PT.
T["pt"] = dict(
    out="Honduras_Climate_API_Onepager_PT_BR.docx",
    title="API de Dados Climáticos de Honduras",
    subtitle="De uma base de dados pronta a um produto vendável em três meses",
    intro="Setenta e seis anos de dados climáticos diários de toda Honduras já estão "
          "baixados, processados e armazenados. O que falta é uma forma de o cliente "
          "chegar até eles. Este é o plano para fechar essa lacuna — e quanto custa em "
          "tempo e dinheiro.",
    h_stand="Onde estamos",
    h_built="O que será construído",
    h_time="Cronograma",
    h_cost="Quanto custa operar",
    h_dec="Duas decisões necessárias agora — jurídicas, grátis hoje, caras depois",
    h_cav="Uma ressalva honesta",
    b1=[("283 células de grade", 1),
        (" cobrem Honduras a cerca de 25 km de resolução. Oito medições diárias por "
         "célula — temperatura, chuva, radiação solar, vento, umidade e demanda "
         "hídrica da cultura — para ", 0),
        ("todos os dias desde 1950", 1), (". Cerca de 8 milhões de registros.", 0)],
    b2=[("A aquisição dos dados está ", 0), ("concluída", 1),
        (": 539 arquivos, 509 MB, já em disco. ", 0),
        ("Total gasto até hoje: menos de US$ 0,10.", 1),
        (" Os dados de origem são gratuitos, de órgãos públicos europeus e "
         "norte-americanos.", 0)],
    b3=[("O gerador mensal de relatórios em PDF ", 0), ("já funciona hoje", 1),
        (". Com identidade visual, em inglês/espanhol/português, oito páginas: chuva e "
         "temperatura frente ao normal, índice de seca, graus-dia de crescimento, "
         "balanço hídrico e mapas do país célula a célula. Gerado em 37 segundos a "
         "partir de dados ao vivo.", 0)],
    b4=[("O que falta é a ", 0), ("camada de serviço", 1),
        (". Hoje o único acesso é uma senha de banco de dados numa rede doméstica — "
         "sem site, sem login, sem nada que um cliente possa chamar. Essa é a lacuna.",
         0)],
    built=[("Um serviço web comercial. O cliente envia uma localização — uma fazenda, "
            "um município ou o país inteiro — junto com o que quer saber, e recebe "
            "dados ou um documento pronto. ", 0),
           ("60 serviços atrás de uma única chave de API", 1),
           (": histórico diário desde 1950; normais climatológicas e o quanto a "
            "temporada atual se afasta delas; indicadores de seca, graus-dia e água para a "
            "cultura; exposição a furacões; previsões sazonais; arquivos prontos para "
            "o modelo de culturas DSSAT; e ", 0),
           ("relatórios PDF automáticos", 1),
           (" — o boletim mensal que já funciona, mais resumos por fazenda e, mais "
            "adiante, a perspectiva sazonal completa no padrão governamental. O acesso "
            "é medido por chave de API, o mesmo modelo que Stripe e OpenAI usam. A "
            "cobrança não está incluída, mas nada precisará ser refeito para "
            "acrescentá-la depois.", 0)],
    assume=[("Um desenvolvedor trabalhando com assistência de IA, ", 0),
            ("seis horas de trabalho concentrado por dia, cinco dias por semana", 1),
            (", começando em 1 de setembro de 2026.", 0)],
    cols=("Marco", "Dias", "Data", "O que pode ser vendido"),
    rows=[
        ("Relatório mensal em PDF", "0", "DISPONÍVEL AGORA",
         "Boletim agroclimático mensal com marca, vendável hoje por assinatura"),
        ("Primeiro serviço no ar", "20", "29 set 2026",
         "Um endpoint seguro, publicado na internet"),
        ("Produto vendável", "60", "24 nov 2026",
         "Histórico, normais, anomalias, indicadores de seca, arquivos de cultura, "
         "exportação em massa"),
        ("Relatórios atrás da API", "68", "5 dez 2026",
         "Relatórios por fazenda e por departamento, sob demanda e autoatendimento"),
        ("Ameaças e seca", "82", "24 dez 2026",
         "Exposição a furacões, alertas municipais de seca"),
        ("Previsões sazonais", "103", "23 jan 2027",
         "Perspectivas de previsão, sujeitas a validação de acurácia"),
        ("Boletim sazonal completo", "156", "8 abr 2027",
         "Produto trimestral em PDF completo, com previsões"),
        ("Encerramento conservador (80% de confiança)", "184", "16 mai 2027", "—"),
    ],
    foot=[("A receita pode começar hoje com o relatório, e com a API em menos de três "
           "meses. ", 1),
          ("Cada data acima depende de sustentar seis horas por dia. A três horas por "
           "dia a API vendável chega em 16 fev 2027 e a entrega completa em 12 nov "
           "2027; no ritmo que este projeto realmente teve até agora (cerca de duas "
           "horas por dia), o vendável escorrega para 6 mai 2027 e a entrega completa "
           "para junho de 2028. ", 0),
          ("A variável é o ritmo, não a dificuldade técnica.", 1)],
    cost=[("US$ 18 por mês", 1), (" durante o desenvolvimento · ", 0),
          ("US$ 26 por mês", 1), (" com o negócio no ar · ", 0),
          ("US$ 51 por mês", 1),
          (" sob carga pesada (um milhão de requisições por dia). O custo corrente de "
           "dados é de cerca de US$ 0,05 por mês. Com esses números, ", 0),
          ("hospedagem não é uma restrição para este projeto.", 1)],
    d1=[("Confirmar uma fonte de dados com licença comercial.", 1),
        (" A ferramenta de download rápido em uso roda num plano gratuito não "
         "comercial, que muito provavelmente não cobre a venda de acesso. Existem três "
         "alternativas viáveis, uma delas já construída e funcionando. Custo pequeno "
         "agora; problema sério se descoberto após o lançamento.", 0)],
    d2=[("Trocar a fonte dos limites cartográficos.", 1),
        (" A fonte prevista para os contornos de departamentos e municípios proíbe a "
         "redistribuição comercial. Existe uma fonte equivalente com licença "
         "comercial. Trocar antes de escrever esse código não custa nada.", 0)],
    caveat=[("A acurácia da previsão sazonal sobre o terreno montanhoso de Honduras "
             "não está comprovada. O plano a mede antes que qualquer previsão chegue a "
             "um cliente. Se a acurácia for baixa, as previsões ficam desligadas e "
             "todo o resto do produto segue intacto — que é exatamente por que elas "
             "estão agendadas por último, atrás do que já gera receita.", 0)],
)


# --- rendering --------------------------------------------------------------

def shade(cell, hexcolor):
    el = OxmlElement("w:shd")
    el.set(qn("w:val"), "clear")
    el.set(qn("w:fill"), hexcolor)
    cell._tc.get_or_add_tcPr().append(el)


def rule_after(paragraph, color=RULE, size=6):
    pbdr = OxmlElement("w:pBdr")
    bottom = OxmlElement("w:bottom")
    bottom.set(qn("w:val"), "single")
    bottom.set(qn("w:sz"), str(size))
    bottom.set(qn("w:space"), "2")
    bottom.set(qn("w:color"), color)
    pbdr.append(bottom)
    paragraph._p.get_or_add_pPr().append(pbdr)


def build(lang):
    c = T[lang]
    doc = Document()

    s = doc.sections[0]
    s.top_margin = Inches(0.38)
    s.bottom_margin = Inches(0.30)
    s.left_margin = Inches(0.58)
    s.right_margin = Inches(0.58)

    normal = doc.styles["Normal"]
    normal.font.name = "Calibri"
    normal.font.size = Pt(8.8)
    normal.font.color.rgb = INK
    pf = normal.paragraph_format
    pf.space_before = Pt(0)
    pf.space_after = Pt(3)
    pf.line_spacing = 1.0

    def rich(parts, size=9, after=3, before=0):
        p = doc.add_paragraph()
        p.paragraph_format.space_after = Pt(after)
        p.paragraph_format.space_before = Pt(before)
        for text, bold in parts:
            r = p.add_run(text)
            r.font.size = Pt(size)
            r.bold = bool(bold)
            r.font.color.rgb = INK
        return p

    def heading(text):
        p = doc.add_paragraph()
        p.paragraph_format.space_before = Pt(5.5)
        p.paragraph_format.space_after = Pt(2)
        r = p.add_run(text.upper())
        r.font.size = Pt(8.5)
        r.bold = True
        r.font.color.rgb = ACCENT
        rule_after(p)
        return p

    def bullet(parts, size=9):
        p = doc.add_paragraph(style="List Bullet")
        p.paragraph_format.space_after = Pt(1.5)
        p.paragraph_format.left_indent = Inches(0.20)
        p.paragraph_format.first_line_indent = Inches(-0.13)
        for text, bold in parts:
            r = p.add_run(text)
            r.font.size = Pt(size)
            r.bold = bool(bold)
            r.font.color.rgb = INK
        return p

    t = doc.add_paragraph()
    t.paragraph_format.space_after = Pt(0)
    r = t.add_run(c["title"])
    r.font.size = Pt(18)
    r.bold = True
    r.font.color.rgb = ACCENT

    st = doc.add_paragraph()
    st.paragraph_format.space_after = Pt(6)
    r = st.add_run(c["subtitle"])
    r.font.size = Pt(10)
    r.italic = True
    r.font.color.rgb = MUTED
    rule_after(st, color="0F5E7A", size=10)

    rich([(c["intro"], 0)], size=9.5, after=2, before=4)

    heading(c["h_stand"])
    for k in ("b1", "b2", "b3", "b4"):
        bullet(c[k])

    heading(c["h_built"])
    rich(c["built"], size=9)

    heading(c["h_time"])
    rich(c["assume"], size=8.5, after=4)

    rows = [c["cols"]] + c["rows"]
    table = doc.add_table(rows=len(rows), cols=4)
    table.style = "Table Grid"
    table.alignment = WD_TABLE_ALIGNMENT.CENTER
    widths = [Inches(1.76), Inches(0.46), Inches(0.94), Inches(4.09)]

    for ri, row in enumerate(rows):
        for ci, text in enumerate(row):
            cell = table.cell(ri, ci)
            cell.width = widths[ci]
            p = cell.paragraphs[0]
            p.paragraph_format.space_before = Pt(0.8)
            p.paragraph_format.space_after = Pt(0.8)
            if ci == 1:
                p.alignment = WD_ALIGN_PARAGRAPH.CENTER
            r = p.add_run(text)
            r.font.size = Pt(8.2)
            if ri == 0:
                r.bold = True
                r.font.color.rgb = RGBColor(0xFF, 0xFF, 0xFF)
                shade(cell, "0F5E7A")
            else:
                r.font.color.rgb = INK
                if ri in (1, 3):        # ships today · first API revenue
                    r.bold = True
                    shade(cell, BAND)
    for ci, w in enumerate(widths):
        for ri in range(len(rows)):
            table.cell(ri, ci).width = w

    rich(c["foot"], size=8.5, before=5, after=2)

    heading(c["h_cost"])
    rich(c["cost"], size=9)

    heading(c["h_dec"])
    bullet(c["d1"])
    bullet(c["d2"])

    heading(c["h_cav"])
    rich(c["caveat"], size=9)

    path = f"{REPO}/{c['out']}"
    doc.save(path)
    return path


if __name__ == "__main__":
    for lg in (sys.argv[1:] or ["en", "es", "pt"]):
        print("wrote", build(lg))
