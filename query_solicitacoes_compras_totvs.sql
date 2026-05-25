WITH sr_sc AS (
    SELECT
        sc.c1_filial,
        sc.c1_num,
        MAX(sr.cr_datalib) AS data_liberacao
    FROM totvs."SC1" sc
    LEFT JOIN totvs."SCR" sr
        ON sr.cr_num = sc.c1_num
       AND sr.cr_filial = sc.c1_filial
       AND sr.cr_tipo = 'SC'
    GROUP BY sc.c1_filial, sc.c1_num
),
sr_pc AS (
    SELECT
        pc.c7_filial,
        pc.c7_num,
        MAX(sr.cr_datalib) AS data_liberacao_pc
    FROM totvs."SC7" pc
    LEFT JOIN totvs."SCR" sr
        ON sr.cr_num = pc.c7_num
       AND sr.cr_filial = pc.c7_filial
       AND sr.cr_tipo = 'IP'
    GROUP BY pc.c7_filial, pc.c7_num
),
dbm_sc AS (
    SELECT
        "DBM_FILIAL" AS dbm_filial,
        "DBM_TIPO" AS dbm_tipo,
        "DBM_NUM" AS dbm_num,
        "DBM_ITEM" AS dbm_item,
        CASE
            WHEN MAX(CASE WHEN "DBM_APROV" = '3' THEN 1 ELSE 0 END) = 1 THEN '3'
            WHEN MAX(CASE WHEN "DBM_APROV" = '2' THEN 1 ELSE 0 END) = 1 THEN '2'
            WHEN MAX(CASE WHEN "DBM_APROV" = '1' THEN 1 ELSE 0 END) = 1 THEN '1'
        END AS dbm_aprov
    FROM totvs."DBM"
    WHERE "DBM_TIPO" = 'SC'
    GROUP BY "DBM_FILIAL", "DBM_TIPO", "DBM_NUM", "DBM_ITEM"
),
cot_ranked AS (
    SELECT
        cot.c8_filial,
        cot.c8_numsc,
        cot.c8_itemsc,
        cot.c8_fornece,
        cot.c8_preco,
        cot.c8_total,
        cot.c8_cond,
        ROW_NUMBER() OVER (
            PARTITION BY cot.c8_filial, cot.c8_numsc, cot.c8_itemsc
            ORDER BY cot.r_e_c_n_o_
        ) AS rn
    FROM totvs."SC8" cot
),
cot_pivot AS (
    SELECT
        c8_filial,
        c8_numsc,
        c8_itemsc,

        MAX(CASE WHEN rn = 1 THEN c8_fornece END) AS c8_fornece_1,
        MAX(CASE WHEN rn = 1 THEN c8_preco END) AS c8_preco_1,
        MAX(CASE WHEN rn = 1 THEN c8_total END) AS c8_total_1,
        MAX(CASE WHEN rn = 1 THEN c8_cond END) AS c8_cond_1,

        MAX(CASE WHEN rn = 2 THEN c8_fornece END) AS c8_fornece_2,
        MAX(CASE WHEN rn = 2 THEN c8_preco END) AS c8_preco_2,
        MAX(CASE WHEN rn = 2 THEN c8_total END) AS c8_total_2,
        MAX(CASE WHEN rn = 2 THEN c8_cond END) AS c8_cond_2,

        MAX(CASE WHEN rn = 3 THEN c8_fornece END) AS c8_fornece_3,
        MAX(CASE WHEN rn = 3 THEN c8_preco END) AS c8_preco_3,
        MAX(CASE WHEN rn = 3 THEN c8_total END) AS c8_total_3,
        MAX(CASE WHEN rn = 3 THEN c8_cond END) AS c8_cond_3,

        MAX(CASE WHEN rn = 4 THEN c8_fornece END) AS c8_fornece_4,
        MAX(CASE WHEN rn = 4 THEN c8_preco END) AS c8_preco_4,
        MAX(CASE WHEN rn = 4 THEN c8_total END) AS c8_total_4,
        MAX(CASE WHEN rn = 4 THEN c8_cond END) AS c8_cond_4,

        MAX(CASE WHEN rn = 5 THEN c8_fornece END) AS c8_fornece_5,
        MAX(CASE WHEN rn = 5 THEN c8_preco END) AS c8_preco_5,
        MAX(CASE WHEN rn = 5 THEN c8_total END) AS c8_total_5,
        MAX(CASE WHEN rn = 5 THEN c8_cond END) AS c8_cond_5
    FROM cot_ranked
    WHERE rn <= 5
    GROUP BY c8_filial, c8_numsc, c8_itemsc
),
ctt_unique AS (
    SELECT
        "CTT_CUSTO" AS ctt_custo,
        MAX("CTT_DESC01") AS ctt_desc01
    FROM totvs."CTT"
    GROUP BY "CTT_CUSTO"
),
sa2_unique AS (
    SELECT
        "A2_COD" AS a2_cod,
        MAX("A2_CGC") AS a2_cgc,
        MAX("A2_NOME") AS a2_nome
    FROM totvs."SA2"
    GROUP BY "A2_COD"
)
SELECT
    COALESCE(TRIM(sc.r_e_c_n_o_::text), ' ') || '|' ||
    COALESCE(TRIM(pc.r_e_c_n_o_::text), ' ') || '|' ||
    COALESCE(TRIM(sc.c1_filial), ' ') || '|' ||
    COALESCE(TRIM(sc.c1_num), ' ') || '|' ||
    COALESCE(TRIM(sc.c1_produto), ' ') || '|' ||
    COALESCE(TRIM(sc.c1_pedido), ' ') || '|' ||
    COALESCE(TRIM(sc.c1_itemped), ' ') || '|' ||
    COALESCE(TRIM(sf.f1_doc), ' ') || '|' ||
    COALESCE(TRIM(sf.f1_dupl), ' ') || '|' ||
    COALESCE(TRIM(sf.f1_emissao), ' ') AS "SUPER_CHAVE",

    sc.c1_filial AS "C1_FILIAL",
    fl.m0_filial AS "M0_FILIAL",
    sc.c1_num AS "C1_NUM",
    sc.c1_emissao AS "EMISSAO_SOL",
    sc.c1_produto AS "C1_PRODUTO",
    sc.c1_um AS "C1_UM",
    sc.c1_descri AS "C1_DESCRI",
    pd."B1_GRUPO" AS "B1_GRUPO",
    gp.bm_desc AS "BM_DESC",
    sc.c1_cc AS "CC_CODIGO_SOLICITACAO",
    cc.ctt_desc01 AS "CC_SOLICITACAO",
    sc.c1_quant AS "C1_QUANT",
    sc.c1_preco AS "C1_PRECO",
    sc.c1_total AS "C1_TOTAL",
    sc.c1_pedido AS "C1_PEDIDO",
    sc.c1_itemped AS "C1_ITEMPED",
    sc.c1_solicit AS "C1_SOLICIT",
    sc.c1_vunit AS "C1_VUNIT",
    sc.c1_aprov AS "APROV_SC",
    sd.d1_total AS "D1_TOTAL",

    CASE dbm_sc.dbm_aprov
        WHEN '1' THEN 'APROVADO'
        WHEN '2' THEN 'PENDENTE'
        WHEN '3' THEN 'REJEITADO'
        ELSE 'SEM ALCADA'
    END AS "DBM_APROV",

    sc.c1_nomapro AS "NOME_APROV_SC",
    sc.c1_unidreq AS "C1_UNIDREQ",
    sc.c1_local AS "C1_LOCAL",

    sf.f1_doc AS "F1_DOC",
    sf.f1_dupl AS "F1_DUPL",
    sf.f1_cond AS "F1_COND",
    sf.f1_emissao AS "F1_EMISSAO",
    sf.f1_status AS "F1_STATUS",
    sf.f1_recbmto AS "F1_RECBMTO",

    sr_sc.data_liberacao AS "DATA_LIBERACAO",
    sr_pc.data_liberacao_pc AS "DATA_LIBERACAO_PC",

    cot_pivot.c8_fornece_1 AS "C8_FORNECE_1",
    cot_pivot.c8_preco_1 AS "C8_PRECO_1",
    cot_pivot.c8_total_1 AS "C8_TOTAL_1",
    cot_pivot.c8_cond_1 AS "C8_COND_1",

    cot_pivot.c8_fornece_2 AS "C8_FORNECE_2",
    cot_pivot.c8_preco_2 AS "C8_PRECO_2",
    cot_pivot.c8_total_2 AS "C8_TOTAL_2",
    cot_pivot.c8_cond_2 AS "C8_COND_2",

    cot_pivot.c8_fornece_3 AS "C8_FORNECE_3",
    cot_pivot.c8_preco_3 AS "C8_PRECO_3",
    cot_pivot.c8_total_3 AS "C8_TOTAL_3",
    cot_pivot.c8_cond_3 AS "C8_COND_3",

    cot_pivot.c8_fornece_4 AS "C8_FORNECE_4",
    cot_pivot.c8_preco_4 AS "C8_PRECO_4",
    cot_pivot.c8_total_4 AS "C8_TOTAL_4",
    cot_pivot.c8_cond_4 AS "C8_COND_4",

    cot_pivot.c8_fornece_5 AS "C8_FORNECE_5",
    cot_pivot.c8_preco_5 AS "C8_PRECO_5",
    cot_pivot.c8_total_5 AS "C8_TOTAL_5",
    cot_pivot.c8_cond_5 AS "C8_COND_5",

    pc.c7_emissao AS "C7_EMISSAO",
    pc.c7_num AS "C7_NUM",
    pc.c7_itemsc AS "C7_ITEMSC",
    pc.c7_quant AS "C7_QUANT",
    pc.c7_preco AS "C7_PRECO",
    pc.c7_total AS "C7_TOTAL",
    pc.c7_medicao AS "C7_MEDICAO",
    pc.c7_penden AS "C7_PENDEN",
    pc.c7_fluxo AS "C7_FLUXO",
    pc.c7_aprov AS "C7_APROV",
    pc.c7_user AS "C7_USER",
    pc.c7_encer AS "C7_ENCER",
    pc.c7_numcot AS "C7_NUMCOT",
    pc.c7_cond AS "C7_COND",
    cond.e4_descri AS "E4_DESCRI",
    pc.c7_descri AS "C7_DESCRI",

    sa.a2_cgc AS "A2_CGC",
    sa.a2_nome AS "A2_NOME",
    pc.c7_xnomcom AS "C7_XNOMCOM",

    cc_2.ctt_desc01 AS "CC_PEDIDO",
    pc.c7_cc AS "CC_CODIGO_PEDIDO",
    pc.r_e_c_n_o_ AS "R_E_C_N_O_PC",
    sc.r_e_c_n_o_ AS "R_E_C_N_O_SC"

FROM totvs."SC1" sc

FULL OUTER JOIN totvs."SC7" pc
    ON pc.c7_filial = sc.c1_filial
   AND pc.c7_num = sc.c1_pedido
   AND pc.c7_item = sc.c1_itemped

LEFT JOIN totvs."SD1" sd
    ON sd.d1_filial = pc.c7_filial
   AND sd.d1_pedido = pc.c7_num
   AND sd.d1_itempc = pc.c7_item

LEFT JOIN totvs."SF1" sf
    ON sf.f1_filial = sd.d1_filial
   AND sf.f1_doc = sd.d1_doc
   AND sf.f1_fornece = sd.d1_fornece

LEFT JOIN sa2_unique sa
    ON sa.a2_cod = pc.c7_fornece

LEFT JOIN totvs."SE4" cond
    ON cond.e4_codigo = pc.c7_cond

LEFT JOIN totvs."SYS_COMPANY_FILIAIS" fl
    ON sc.c1_filial = fl.m0_codfil

LEFT JOIN totvs."SB1" pd
    ON pd."B1_COD" = sc.c1_produto

LEFT JOIN totvs."SBM" gp
    ON gp.bm_grupo = pd."B1_GRUPO"

LEFT JOIN ctt_unique cc
    ON cc.ctt_custo = sc.c1_cc

LEFT JOIN ctt_unique cc_2
    ON cc_2.ctt_custo = pc.c7_cc

LEFT JOIN sr_sc
    ON sr_sc.c1_num = sc.c1_num
   AND sr_sc.c1_filial = sc.c1_filial

LEFT JOIN sr_pc
    ON sr_pc.c7_num = pc.c7_num
   AND sr_pc.c7_filial = pc.c7_filial

LEFT JOIN dbm_sc
    ON dbm_sc.dbm_filial = sc.c1_filial
   AND dbm_sc.dbm_tipo = 'SC'
   AND dbm_sc.dbm_num = sc.c1_num
   AND dbm_sc.dbm_item = sc.c1_item

LEFT JOIN cot_pivot
    ON cot_pivot.c8_filial = sc.c1_filial
   AND cot_pivot.c8_numsc = sc.c1_num
   AND cot_pivot.c8_itemsc = sc.c1_item
