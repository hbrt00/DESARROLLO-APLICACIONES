DECLARE var_fecha_actual DATE;        --** Fecha actual
DECLARE var_fecha_inicio DATE;        --** Fecha del mes anterior para lectura de datos en Silver
DECLARE var_fecha_inicio_insert DATE; --** Fecha para insercion de datos para Gold s4_fill_rate

SET var_fecha_actual = CURRENT_DATE('America/Lima');
SET var_fecha_inicio = DATE_TRUNC(DATE_SUB(var_fecha_actual, INTERVAL 1 MONTH), MONTH);  
SET var_fecha_inicio_insert = CASE
  WHEN EXTRACT(DAY FROM var_fecha_actual) <= 8 THEN DATE_TRUNC(DATE_SUB(var_fecha_actual, INTERVAL 1 MONTH), MONTH)  --** Primer día del mes anterior, validacion corte mes el 8.
  ELSE DATE_TRUNC(var_fecha_actual, MONTH)  --** Primer día del mes actual si la fecha es mayor al 8 del mes actual (Ejm: aaaa-mm-01)
END;


BEGIN
-----------------------------------------
--DEFINIMOS VBRK YA QUE ES UN CRUCE DE DOS TABLAS
CREATE OR REPLACE temp table VBRK_I AS 
  SELECT DISTINCT a.fec_documento
    ,b.id_documento_origen
    ,b.id_documento
    ,b.cod_clase_documento
    ,b.flg_documento_cancelado
  FROM `{silver_project_id}.slv_modelo_ventas.documento_cabecera` as a
  INNER JOIN `{silver_project_id}.slv_modelo_ventas.s4_documento_cabecera_aux` as b
  ON a.id_documento   = b.id_documento 
    AND a.des_origen  = "SAPS4"
  WHERE a.periodo >= var_fecha_inicio 
    AND b.periodo >= var_fecha_inicio 
    AND a.periodo <=  var_fecha_actual 
    AND b.periodo <=  var_fecha_actual 
;


CREATE OR REPLACE temp table VBAK_I AS 
  SELECT 
    cod_segmento_cliente,
    fec_creacion,
    cod_sociedad_factura,
    cod_sector_comercial,
    id_interlocutor,
    cod_condicion_expedicion,
    cod_oficina_venta,
    cod_grupo_vendedor,
    id_pedido,
    id_pedido_origen,
    cod_clase_documento,
    cod_organizacion_venta,
    cod_canal_distribucion,
    cod_bloqueo_entrega
  FROM `{silver_project_id}.slv_modelo_ventas.s4_pedido_cabecera`
  WHERE 
  fec_creacion >= var_fecha_inicio 
    AND periodo >= var_fecha_inicio 
    AND fec_creacion <=var_fecha_actual 
    AND cod_sociedad_factura='PE11' AND cod_canal_distribucion='01' --SOLO NOS QUEDAMOS CON ALICORP PERU Y CANAL NACIONAL
    AND cod_clase_documento in ("ZPAD","ZPED","ZEDI","ZPAN","ZRCH","ZEXP","ZFAD","ZPOE","ZBCI","ZBCR","ZBDI")
;


---#####################################################################################
---## PASO 2: obtendremos la información de la VBAP (Posición de facturas) con las que se evaluará el fill rate como productos solicitados
---## VBAP1 (Aquellas posiciones con rechazo)
---## VBAP2 (Aquellas posiciones sin rechazo)
---#####################################################################################
CREATE OR REPLACE temp table VBAP_I AS 
  SELECT
    a.cod_unidad_medida_base,
    a.id_pedido,
    a.cod_motivo_rechazo,
    a.est_procesamiento,
    a.est_entrega,
    a.num_correlativo,
    a.cnt_acumulada_venta,
    a.cod_centro,
    a.cod_punto_expedicion,
    a.cod_jerarquia_material,
    a.id_material,
    a.cod_marca,
    a.cod_negocio,
    a.cod_subnegocio,
    a.cod_unidad_medida_venta,
    a.cod_documento_modelo,
    a.num_correlativo_documento_modelo,
    a.mnt_peso_neto,
    a.cod_unidad_medida_peso,
    a.mnt_neto_pedido,
    a.cod_moneda
  FROM `{silver_project_id}.slv_modelo_ventas.s4_pedido_detalle` as a
  INNER JOIN VBAK_I as b
  ON a.id_pedido=b.id_pedido 
    AND a.cod_motivo_rechazo is not null
    AND a.est_procesamiento = "C"
    AND a.est_entrega = "B"
  WHERE a.periodo >= var_fecha_inicio 
    AND a.periodo <=var_fecha_actual
;


CREATE OR REPLACE temp table VBAP2_I AS 
  SELECT
    DISTINCT
    a.cod_unidad_medida_base,
    a.id_pedido,
    a.cod_motivo_rechazo,
    a.est_procesamiento,
    a.est_entrega,
    a.num_correlativo,
    a.cnt_acumulada_venta,
    a.cod_centro,
    a.cod_punto_expedicion,
    a.cod_jerarquia_material,
    a.id_material,
    a.cod_marca,
    a.cod_negocio,
    a.cod_subnegocio,
    a.cod_unidad_medida_venta,
    a.cod_documento_modelo,
    a.num_correlativo_documento_modelo,
    a.mnt_peso_neto,
    a.cod_unidad_medida_peso,
    a.mnt_neto_pedido,
    a.cod_moneda
  FROM `{silver_project_id}.slv_modelo_ventas.s4_pedido_detalle` as a
  INNER JOIN VBAK_I as b
  ON a.id_pedido=b.id_pedido 
    AND a.cod_motivo_rechazo is null 
    AND a.est_procesamiento = "C"
    AND a.est_entrega = "C" 
  WHERE a.periodo >= var_fecha_inicio 
    AND a.periodo <=var_fecha_actual 
;


---#####################################################################################
---## PASO 3: ESTADOS DE LA LIPS #######################################################
---#####################################################################################
CREATE OR REPLACE temp table LIPS as
SELECT 
  a.cnt_entregada_unidad_venta,
  a.cod_unidad_medida,
  a.cod_unidad_medida_venta,
  a.cod_documento_modelo,
  a.num_correlativo_documento_modelo,
  a.est_posicion,
  a.est_facturacion,
  a.id_documento_entrega,
  a.num_correlativo,
  a.id_material,
  a.mnt_peso_neto AS mnt_peso_entrega
FROM `{silver_project_id}.slv_modelo_transporte.documento_entrega_detalle` as a
INNER JOIN VBAP_I as b
ON CONCAT('PVE-', 'SAPS4-', a.cod_documento_modelo) = b.id_pedido
  AND a.num_correlativo_documento_modelo  = b.num_correlativo 
  AND a.cnt_entregada_unidad_venta  <>  0
--and a.est_posicion = "C"  --version antigua hasta 05/03/2024
--and (a.est_facturacion = "C" or a.est_facturacion is NULL) --version antigua hasta 05/03/2024
  AND (a.est_facturacion = "C" or (a.est_facturacion is NULL and a.est_posicion = "C" ))
WHERE a.periodo >= var_fecha_inicio 
AND a.periodo <=  var_fecha_actual
AND a.cnt_entregada_unidad_venta  <>  0
AND (a.est_facturacion = "C" or (a.est_facturacion is NULL and a.est_posicion = "C" ))
;


CREATE OR REPLACE temp table  LIPS1 as
SELECT
  a.cnt_entregada_unidad_venta,
  a.cod_unidad_medida,
  a.cod_unidad_medida_venta,
  a.cod_documento_modelo,
  a.num_correlativo_documento_modelo,
  a.est_posicion,
  a.est_facturacion,
  a.id_documento_entrega,
  a.num_correlativo,
  a.id_material,
  a.mnt_peso_neto AS mnt_peso_entrega
FROM `{silver_project_id}.slv_modelo_transporte.documento_entrega_detalle` as a
INNER JOIN VBAP2_I as b
ON CONCAT('PVE-', 'SAPS4-', a.cod_documento_modelo) = b.id_pedido 
  AND a.num_correlativo_documento_modelo  = b.num_correlativo 
  AND a.cnt_entregada_unidad_venta  <>  0
--and a.est_posicion = "C"  --version antigua hasta 05/03/2024
--and (a.est_facturacion = "C" or a.est_facturacion is NULL) --version antigua hasta 05/03/2024
  AND (a.est_facturacion = "C" or (a.est_facturacion is NULL and a.est_posicion = "C" ))
WHERE a.periodo >= var_fecha_inicio 
  AND a.periodo <=  var_fecha_actual 
  AND a.fec_creacion <= var_fecha_actual
  AND a.cnt_entregada_unidad_venta  <>  0
  AND (a.est_facturacion = "C" or (a.est_facturacion is NULL and a.est_posicion = "C" ))
;


---#####################################################################################
---## PASO 4: LIMPIEZA DE CODIGOS A NO USARSE ##########################################
---#####################################################################################
CREATE OR REPLACE temp table VBAP_Q as
  SELECT 
    a.*
  FROM VBAP_I as a
  INNER JOIN LIPS as c
  ON a.num_correlativo  = c.num_correlativo_documento_modelo 
  --POSNR ;VGPOS
    AND a.id_pedido=CONCAT('PVE-', 'SAPS4-', c.cod_documento_modelo)
;
--  SELECT * FROM VBAP_Q;


CREATE OR REPLACE temp table LIPS3 as
 SELECT DISTINCT 
  cod_documento_modelo,
  num_correlativo_documento_modelo 
 FROM  `{silver_project_id}.slv_modelo_transporte.documento_entrega_detalle` as b
 WHERE b.cnt_entregada_unidad_venta <>  0 
  AND (b.est_facturacion="A" and b.est_posicion in ("A","B")) --cambio de OR por AND para el mapeo de entregas en cero
  AND b.periodo >= var_fecha_inicio 
  AND b.periodo <=  var_fecha_actual
;


CREATE OR REPLACE TEMP TABLE VBAP_X AS
  SELECT  
    a.*
  FROM VBAP_I a
  LEFT JOIN VBAP_Q b
  ON a.id_pedido = b.id_pedido
    AND a.num_correlativo = b.num_correlativo
  WHERE b.id_pedido IS NULL 
    AND b.num_correlativo IS NULL
;


CREATE OR REPLACE TEMP TABLE VBAP_Y AS
  SELECT 
    a.*,
    c.num_correlativo_documento_modelo AS num_correlativo_documento_modelo_l,
    c.cod_documento_modelo AS cod_documento_modelo_l
  FROM      VBAP_X AS a
  LEFT JOIN LIPS3  AS c
  ON    a.num_correlativo   = c.num_correlativo_documento_modelo 
    AND a.id_pedido         = CONCAT('PVE-', 'SAPS4-', c.cod_documento_modelo)
  WHERE c.num_correlativo_documento_modelo IS NULL 
    AND c.cod_documento_modelo IS NULL
;


CREATE OR REPLACE TEMP TABLE VBAP1 AS
  SELECT
    a.cod_unidad_medida_base,
    a.id_pedido,
    a.cod_motivo_rechazo,
    a.est_procesamiento,
    a.est_entrega,
    a.num_correlativo,
    a.cnt_acumulada_venta,
    a.cod_centro,
    a.cod_punto_expedicion,
    a.cod_jerarquia_material,
    a.id_material,
    a.cod_marca,
    a.cod_negocio,
    a.cod_subnegocio,
    a.cod_unidad_medida_venta,
    a.cod_documento_modelo,
    a.num_correlativo_documento_modelo,
    a.mnt_peso_neto,
    a.cod_unidad_medida_peso,
    a.mnt_neto_pedido,
    a.cod_moneda
  FROM `{silver_project_id}.slv_modelo_ventas.s4_pedido_detalle` AS a
  INNER JOIN VBAK_I AS b
  ON a.id_pedido = b.id_pedido 
    AND a.cod_motivo_rechazo IS NOT NULL 
    AND (a.est_procesamiento = 'C' OR a.est_procesamiento IS NULL)
    AND (a.est_entrega = 'A' OR a.est_entrega IS NULL)
  WHERE a.periodo >= var_fecha_inicio 
    AND a.periodo <=  var_fecha_actual

  UNION ALL

  SELECT
    a.cod_unidad_medida_base,
    a.id_pedido,
    a.cod_motivo_rechazo,
    a.est_procesamiento,
    a.est_entrega,
    a.num_correlativo,
    a.cnt_acumulada_venta,
    a.cod_centro,
    a.cod_punto_expedicion,
    a.cod_jerarquia_material,
    a.id_material,
    a.cod_marca,
    a.cod_negocio,
    a.cod_subnegocio,
    a.cod_unidad_medida_venta,
    a.cod_documento_modelo,
    a.num_correlativo_documento_modelo,
    a.mnt_peso_neto,
    a.cod_unidad_medida_peso,
    a.mnt_neto_pedido,
    a.cod_moneda
  FROM VBAP_Y AS a
;


CREATE OR REPLACE TEMP TABLE VBAP AS
  SELECT 
    a.*,
    c.num_correlativo_documento_modelo AS num_correlativo_documento_modelo_l,
    c.cod_documento_modelo AS cod_documento_modelo_l
  FROM      VBAP_Q  a
  LEFT JOIN LIPS3   c
  ON    a.num_correlativo   = c.num_correlativo_documento_modelo 
    AND   a.id_pedido       = CONCAT('PVE-', 'SAPS4-', c.cod_documento_modelo)
  WHERE c.num_correlativo_documento_modelo IS NULL 
    AND   c.cod_documento_modelo IS NULL
;


---------------------------------------SE PUEDE ELIMINAR CUANDO SE DEJE LA PRIMERA FASE
CREATE OR REPLACE temp table VBAP2_PREVIA as
  SELECT 
    DISTINCT
    a.cod_unidad_medida_base,
    a.id_pedido,
    a.cod_motivo_rechazo,
    a.est_procesamiento,
    a.est_entrega,
    a.num_correlativo,
    a.cnt_acumulada_venta,
    a.cod_centro,
    a.cod_punto_expedicion,
    a.cod_jerarquia_material,
    a.id_material,
    a.cod_marca,
    a.cod_negocio,
    a.cod_subnegocio,
    a.cod_unidad_medida_venta,
    a.cod_documento_modelo,
    a.num_correlativo_documento_modelo,
    a.mnt_peso_neto,
    a.cod_unidad_medida_peso,
    a.mnt_neto_pedido,
    a.cod_moneda 
  FROM        VBAP2_I as a
  INNER JOIN  LIPS1 as b
  ON a.id_pedido  = 'PVE-' || 'SAPS4-' || b.cod_documento_modelo 
    AND a.num_correlativo = b.num_correlativo_documento_modelo
;


CREATE OR REPLACE temp table VBAP2 as
  SELECT 
    DISTINCT
    a.cod_unidad_medida_base,
    a.id_pedido,
    a.cod_motivo_rechazo,
    a.est_procesamiento,
    a.est_entrega,
    a.num_correlativo,
    a.cnt_acumulada_venta,
    a.cod_centro,
    a.cod_punto_expedicion,
    a.cod_jerarquia_material,
    a.id_material,
    a.cod_marca,
    a.cod_negocio,
    a.cod_subnegocio,
    a.cod_unidad_medida_venta,
    a.cod_documento_modelo,
    a.num_correlativo_documento_modelo,
    a.mnt_peso_neto,
    a.cod_unidad_medida_peso,
    a.mnt_neto_pedido,
    a.cod_moneda 
  FROM VBAP2_PREVIA AS a
  LEFT JOIN LIPS3 AS c
  ON a.num_correlativo=c.num_correlativo_documento_modelo 
  --POSNR ;VGPOS
    AND a.id_pedido='PVE-' || 'SAPS4-' ||c.cod_documento_modelo
  --WHERE b.periodo >= var_fecha_inicio 
  WHERE c.num_correlativo_documento_modelo is null 
    AND c.cod_documento_modelo is null
;


---#####################################################################################
---## PASO 1: TABLA INICIAL ############################################################
---#####################################################################################
CREATE OR REPLACE temp table VBAK as
  SELECT DISTINCT * FROM
    (SELECT 
      a.*
    FROM VBAK_I as a
    INNER JOIN VBAP as b
    ON a.id_pedido  = b.id_pedido

    UNION ALL
    
    SELECT 
      a.*
    FROM VBAK_I as a
    INNER JOIN VBAP1 as c
    ON a.id_pedido  = c.id_pedido


    UNION ALL
    
    SELECT 
      a.*
    FROM VBAK_I as a
    INNER JOIN VBAP2 as c
    ON a.id_pedido  = c.id_pedido)
  ---Se realiza ajuste para documentos ZRCH en pedidos que no deben considerarse de ind_bloque=2
  WHERE cod_clase_documento not in ("ZRCH")  
;

---#####################################################################################
---## PASO 6: OBTENER DATOS DE TABLAS ADICIONALES ######################################
---#####################################################################################
CREATE OR REPLACE temp table LIKP as
  SELECT DISTINCT
    a.id_documento_entrega,
    a.id_documento_entrega_origen,
    a.mnt_neto_pedido as valor_neto_entrega
  FROM `{silver_project_id}.slv_modelo_transporte.documento_entrega_cabecera` as a
  INNER JOIN LIPS as b
  ON a.id_documento_entrega=b.id_documento_entrega
  WHERE a.periodo >= var_fecha_inicio 
    AND a.periodo <= var_fecha_actual
;


CREATE OR REPLACE temp table  LIKP1 as
  SELECT DISTINCT
    a.id_documento_entrega,
    a.id_documento_entrega_origen
  FROM `{silver_project_id}.slv_modelo_transporte.documento_entrega_cabecera` as a
  INNER JOIN LIPS1 as b
  on a.id_documento_entrega=b.id_documento_entrega
  WHERE a.periodo >= var_fecha_inicio 
    AND a.periodo <= var_fecha_actual
;


CREATE OR REPLACE temp table  TVPOD as
  SELECT 
    a.num_correlativo,
    a.cnt_desviacion,
    a.id_documento_tvpod_origen,
    a.cod_motivo_desviacion,
    a.fec_pedido,
    a.fec_notificacion,
    a.cod_unidad_medida_venta,
    a.cod_unidad_medida_base,
    a.id_material
  FROM `{silver_project_id}.slv_modelo_ventas.documento_tvpod` as a
  INNER JOIN LIPS as b
  on 'DCE-' || 'SAPS4-' || a.id_documento_tvpod_origen =b.id_documento_entrega and a.num_correlativo=b.num_correlativo
;


CREATE OR REPLACE temp table TVPOD1 as
  SELECT 
    a.num_correlativo,
    a.cnt_desviacion,
    a.id_documento_tvpod_origen,
    a.cod_motivo_desviacion,
    a.fec_pedido,
    a.fec_notificacion,
    a.cod_unidad_medida_venta,
    a.cod_unidad_medida_base,
    a.id_material
  FROM `{silver_project_id}.slv_modelo_ventas.documento_tvpod` as a
  INNER JOIN LIPS1 as b
  on 'DCE-' || 'SAPS4-' || a.id_documento_tvpod_origen =b.id_documento_entrega and a.num_correlativo=b.num_correlativo
;


CREATE OR REPLACE temp table  VBRP as
  SELECT 
    a.cod_unidad_medida_base,
    a.cod_unidad_medida_venta,
    a.cod_documento_modelo,
    a.id_documento,
    a.cnt_material,
    a.id_material, 
    a.num_correlativo,
    a.num_correlativo_documento_modelo,
    a.mnt_peso_neto as mnt_peso_factura,
    a.mnt_total_neto as valor_factura
  FROM `{silver_project_id}.slv_modelo_ventas.s4_documento_detalle_aux` as a
  INNER JOIN LIPS as b
  on 'DCE-' || 'SAPS4-' || a.cod_documento_modelo=b.id_documento_entrega
  and a.num_correlativo_documento_modelo= b.num_correlativo
  WHERE a.periodo >= var_fecha_inicio 
    AND a.periodo <= var_fecha_actual
;


CREATE OR REPLACE temp table VBRP1 as
  SELECT 
    a.cod_unidad_medida_base,
    a.cod_unidad_medida_venta,
    a.cod_documento_modelo,
    a.id_documento,
    a.cnt_material,
    a.id_material,
    a.num_correlativo,
    a.num_correlativo_documento_modelo,
    a.mnt_peso_neto as mnt_peso_factura,
    a.mnt_total_neto as valor_factura
  FROM `{silver_project_id}.slv_modelo_ventas.s4_documento_detalle_aux` as a
  INNER JOIN LIPS1 as b
  on 'DCE-' || 'SAPS4-' || a.cod_documento_modelo=b.id_documento_entrega and a.num_correlativo_documento_modelo= b.num_correlativo
  WHERE a.periodo >= var_fecha_inicio
    AND a.periodo <= var_fecha_actual
;


--LA VBRK NO FUE LIMPIADA POR CAMPOS (REVISAR SI FUNCIONA)
CREATE OR REPLACE temp table VBRK as
  SELECT 
    fec_documento,
    id_documento_origen,
    a.id_documento,
    flg_documento_cancelado,
    cod_clase_documento
  FROM VBRK_I as a
  INNER JOIN VBRP as b
  on 'DOC-' || 'SAPS4-' ||a.id_documento_origen=b.id_documento
  and flg_documento_cancelado =  false
  and cod_clase_documento in ("ZF01", "ZF06", "ZF07", "ZF08", "ZFEX")
;


CREATE OR REPLACE temp table VBRK1 as
  SELECT 
    fec_documento,
    id_documento_origen,
    a.id_documento,
    flg_documento_cancelado,
    cod_clase_documento
  FROM VBRK_I as a
  INNER JOIN VBRP1 as b
  on 'DOC-' || 'SAPS4-' ||a.id_documento_origen=b.id_documento
  and flg_documento_cancelado=false --flg_documento_cancelado IS NULL
  and cod_clase_documento in ("ZF01", "ZF06", "ZF07", "ZF08", "ZFEX")
;


CREATE OR REPLACE temp table VBKD as
  --el cruce ya se realizo en la tabla
  SELECT 
    a.cod_zona_cliente,
    a.cod_grupo_cliente,
    a.cod_grupo_condicion,
    a.cod_grupo_precio,
    a.cod_categoria_precio,
    a.cod_condicion_pago,
    a.num_correlativo,
    a.id_pedido
  FROM `{silver_project_id}.slv_modelo_ventas.s4_pedido_detalle_comercial` as a
  INNER JOIN VBAK as b
  on a.id_pedido=b.id_pedido
  WHERE a.periodo >= var_fecha_inicio 
    AND a.periodo <= var_fecha_actual
;


CREATE OR REPLACE TEMP TABLE MARA AS
  SELECT DISTINCT
    ma.cod_material_antiguo,
    ma.COD_DUENIO_MARCA,
    ma.DES_DUENIO_MARCA,
    a.id_material_origen,
    a.id_material,
    a.cod_tipo_material,
    a.fec_creacion,
    a.cod_unidad_medida_base
  FROM `{silver_project_id}.slv_modelo_material.material` AS a
  LEFT JOIN `{silver_project_id}.slv_modelo_material.s4_material_aux` AS ma
  ON  a.id_material       = ma.id_material 
  AND a.des_origen        = 'SAPS4' 
  AND a.cod_tipo_material IN ('ZFER', 'ZHAW')
  INNER JOIN VBAP AS b
  ON a.id_material = b.id_material
;


CREATE OR REPLACE TEMP TABLE MARA1 AS
  SELECT DISTINCT
    ma.cod_material_antiguo,
    ma.COD_DUENIO_MARCA,
    ma.DES_DUENIO_MARCA,
    a.id_material_origen,
    a.cod_tipo_material,
    a.fec_creacion,
    a.cod_unidad_medida_base,
    a.id_material
  FROM      `{silver_project_id}.slv_modelo_material.material`        AS a
  LEFT JOIN `{silver_project_id}.slv_modelo_material.s4_material_aux` AS ma
  ON a.id_material        = ma.id_material 
  AND a.des_origen        = 'SAPS4' 
  AND a.cod_tipo_material IN ('ZFER', 'ZHAW')
  INNER JOIN VBAP1 AS b
  ON a.id_material = b.id_material
;


CREATE OR REPLACE temp table  MARA2 as
  SELECT 
    DISTINCT
    ma.cod_material_antiguo,
    ma.COD_DUENIO_MARCA,
    ma.DES_DUENIO_MARCA,
    a.id_material_origen,
    a.cod_tipo_material,
    a.fec_creacion,
    a.cod_unidad_medida_base,
    a.id_material
  FROM `{silver_project_id}.slv_modelo_material.material` a
  LEFT JOIN `{silver_project_id}.slv_modelo_material.s4_material_aux` ma
  on a.id_material = ma.id_material and a.des_origen = 'SAPS4' and a.cod_tipo_material in ("ZFER","ZHAW")
  INNER JOIN VBAP2 as b
  on a.id_material=b.id_material
;


CREATE OR REPLACE temp table MARM as
  SELECT
    DISTINCT
    a.num_denominador_conversion,
    a.num_numerador_conversion,
    a.id_material,
    a.id_material_origen,
    a.cod_unidad_medida
  FROM `{silver_project_id}.slv_modelo_material.material_unidad_medida` as a
  INNER JOIN MARA as b
  ON a.id_material_origen=b.id_material_origen 
  AND a.des_origen='SAPS4'
;

CREATE OR REPLACE temp table MARM_COMPLEMENTO as
  SELECT * FROM MARM
;


CREATE OR REPLACE temp table MARM1 as
  SELECT
    DISTINCT
    a.num_denominador_conversion,
    a.num_numerador_conversion,
    a.id_material,
    a.id_material_origen,
    a.cod_unidad_medida 
  FROM `{silver_project_id}.slv_modelo_material.material_unidad_medida`  as a
  INNER JOIN MARA1 as b
  ON    a.id_material_origen    =   b.id_material_origen 
    AND a.des_origen            =   'SAPS4'
;


CREATE OR REPLACE temp table MARM1_COMPLEMENTO as
  SELECT * FROM MARM1
;


CREATE OR REPLACE temp table MARM2 as
  SELECT
    DISTINCT
    a.num_denominador_conversion,
    a.num_numerador_conversion,
    a.id_material,
    a.id_material_origen,
    a.cod_unidad_medida 
  FROM `{silver_project_id}.slv_modelo_material.material_unidad_medida` as a
  INNER JOIN MARA2 as b
  ON    a.id_material_origen  = b.id_material_origen 
    AND a.des_origen          = 'SAPS4'
;

CREATE OR REPLACE temp table MARM2_COMPLEMENTO as
  SELECT * FROM MARM2
;

---#####################################################################################
---## PASO 5: OBTENER RECHAZOS #########################################################
---#####################################################################################
CREATE OR REPLACE temp table VBFA as
  SELECT DISTINCT
    a.cod_documento_anterior,
    a.cod_categoria_documento_siguiente,
    a.cod_documento_siguiente
  FROM `{silver_project_id}.slv_modelo_ventas.flujo_documento` as a
  INNER JOIN VBRK as b
  ON a.cod_documento_anterior= b.id_documento_origen 
    AND a.cod_categoria_documento_siguiente="H" 
  WHERE a.periodo >= var_fecha_inicio 
    AND a.periodo <= var_fecha_actual
  
  UNION ALL
  
  SELECT DISTINCT
    a.cod_documento_anterior,
    a.cod_categoria_documento_siguiente,
    a.cod_documento_siguiente
  FROM `{silver_project_id}.slv_modelo_ventas.flujo_documento` as a
  INNER JOIN VBRK1 as b
  ON a.cod_documento_anterior= b.id_documento_origen and 
    a.cod_categoria_documento_siguiente="H"  
  WHERE a.periodo >= var_fecha_inicio 
    AND a.periodo <= var_fecha_actual
;


CREATE OR REPLACE temp table  VBAK3 as
  SELECT 
    a.cod_segmento_cliente,
    a.fec_creacion,
    a.cod_sociedad_factura,
    a.cod_sector_comercial,
    a.id_interlocutor,
    a.cod_condicion_expedicion,
    a.cod_oficina_venta,
    a.cod_grupo_vendedor,
    a.id_pedido,
    a.id_pedido_origen,
    a.cod_clase_documento,
    a.cod_organizacion_venta,
    a.cod_canal_distribucion,
    a.cod_bloqueo_entrega,
    a.cod_motivo_pedido
  FROM `{silver_project_id}.slv_modelo_ventas.s4_pedido_cabecera` as a
  INNER JOIN VBFA as b
  ON a.id_pedido_origen=b.cod_documento_siguiente
    AND a.cod_clase_documento="ZRCH"
  WHERE a.fec_creacion >= var_fecha_inicio 
    AND a.periodo >= var_fecha_inicio --AND a.fec_creacion <=var_fecha_actual 
;


CREATE OR REPLACE temp table VBAP3 as
  SELECT
    a.cod_unidad_medida_base,
    a.id_pedido,
    a.cod_motivo_rechazo,
    a.est_procesamiento,
    a.est_entrega,
    a.num_correlativo,
    a.cnt_acumulada_venta,
    a.cod_centro,
    a.cod_punto_expedicion,
    a.cod_jerarquia_material,
    a.id_material,
    a.cod_marca,
    a.cod_negocio,
    a.cod_subnegocio,
    a.cod_unidad_medida_venta,
    a.cod_documento_modelo,
    a.num_correlativo_documento_modelo,
    a.mnt_peso_neto as mnt_peso_rechazo,
    a.mnt_neto_pedido as valor_rechazo 
  FROM `{silver_project_id}.slv_modelo_ventas.s4_pedido_detalle` as a
  INNER JOIN VBAK3 as b
  on a.id_pedido  = b.id_pedido and a.cod_motivo_rechazo is null
  WHERE a.periodo >= var_fecha_inicio 
    AND a.periodo <= var_fecha_actual
;


CREATE OR REPLACE temp table  VBFA_NUEVA as
  SELECT DISTINCT
    a.cod_documento_anterior,
    a.cod_categoria_documento_siguiente,
    a.cod_documento_siguiente
  FROM `{silver_project_id}.slv_modelo_ventas.flujo_documento` as a
  INNER JOIN VBRK1 as b
  ON a.cod_documento_anterior= b.id_documento_origen and 
    a.cod_categoria_documento_siguiente="H" 
  WHERE a.periodo >= var_fecha_inicio 
    AND a.periodo <= var_fecha_actual
;


CREATE OR REPLACE temp table  VBAK4 as
  SELECT 
    a.cod_segmento_cliente,
    a.fec_creacion,
    a.cod_sociedad_factura,
    a.cod_sector_comercial,
    a.id_interlocutor,
    a.cod_condicion_expedicion,
    a.cod_oficina_venta,
    a.cod_grupo_vendedor,
    a.id_pedido,
    a.id_pedido_origen,
    a.cod_clase_documento,
    a.cod_organizacion_venta,
    a.cod_canal_distribucion,
    a.cod_bloqueo_entrega,
    a.cod_motivo_pedido
  FROM `{silver_project_id}.slv_modelo_ventas.s4_pedido_cabecera` as a
  INNER JOIN VBFA_NUEVA as b
  ON a.id_pedido_origen=b.cod_documento_siguiente
  AND a.cod_clase_documento="ZRCH"
  WHERE a.fec_creacion >= var_fecha_inicio 
    AND a.periodo >= var_fecha_inicio --AND a.fec_creacion <=var_fecha_actual 
;


CREATE OR REPLACE temp table VBAP4 as
  SELECT
    a.cod_unidad_medida_base,
    a.id_pedido,
    a.cod_motivo_rechazo,
    a.est_procesamiento,
    a.est_entrega,
    a.num_correlativo,
    a.cnt_acumulada_venta,
    a.cod_centro,
    a.cod_punto_expedicion,
    a.cod_jerarquia_material,
    a.id_material,
    a.cod_marca,
    a.cod_negocio,
    a.cod_subnegocio,
    a.cod_unidad_medida_venta,
    a.cod_documento_modelo,
    a.num_correlativo_documento_modelo,
    a.mnt_peso_neto as mnt_peso_rechazo,
    a.mnt_neto_pedido as valor_rechazo  
  FROM `{silver_project_id}.slv_modelo_ventas.s4_pedido_detalle` as a
  INNER JOIN VBAK4 as b
  on a.id_pedido=b.id_pedido and a.cod_motivo_rechazo is null
  WHERE a.periodo >= var_fecha_inicio
    AND a.periodo <= var_fecha_actual
;


CREATE OR REPLACE temp table MVKE AS 
  SELECT 
    id_materiaL,
    cod_organizacion_venta,
    cod_canal_distribucion,
    cod_unidad_comercial
  FROM `{silver_project_id}.slv_modelo_material.material_organizacion_venta`
;


CREATE OR REPLACE temp table KNA1 AS 
  SELECT *
  FROM `{silver_project_id}.slv_modelo_interlocutor.interlocutor`
  WHERE des_origen='SAPS4'
;


CREATE OR REPLACE temp table VBPA AS 
  SELECT 
    SUBSTR(id_documento,11,20) id_documento,
    id_cliente,
    cod_funcion_socio,
    num_correlativo,
    tip_documento
  FROM `{silver_project_id}.slv_modelo_ventas.s4_documento_interlocutor`
  WHERE periodo >= var_fecha_inicio 
  AND periodo <= var_fecha_actual
  AND des_origen='SAPS4'
;


CREATE OR REPLACE temp table VBPA2 AS 
  SELECT 
    SUBSTR(id_documento,11,20) id_documento,
    id_cliente,
    cod_funcion_socio,
    num_correlativo,
    tip_documento
  FROM `{silver_project_id}.slv_modelo_ventas.s4_documento_interlocutor`
  WHERE periodo >= var_fecha_inicio 
  AND periodo <= var_fecha_actual
  AND des_origen='SAPS4'
;


---****************************************************************************************
---********* GENERACION DE CAMPOS INDEPENDIENTES TABLA PREVIA FINAL 1 *********************
CREATE OR REPLACE temp table INDEPENDIENTE_TABLA1 AS 
  SELECT 
    DISTINCT
    vbap_aux.mnt_peso_neto,
    vbap_aux.cod_unidad_medida_peso,
    vbap_aux.mnt_neto_pedido,
    vbap_aux.cod_moneda,
    ---
    LIPS.id_documento_entrega,
    LIPS.num_correlativo num_correlativo_lips,
    LIPS.num_correlativo_documento_modelo,
    LIPS.cod_documento_modelo,
    ---
    VBAK.cod_sociedad_factura sociedad,
    VBAK.cod_organizacion_venta organizacion_venta,
    VBAK.cod_canal_distribucion canal,
    VBAK.cod_sector_comercial sector,
    REPLACE(VBAK.id_interlocutor,'ITL-SAPS4-','') cliente,
    CASE WHEN VBPA_NIVEL1.id_documento=VBAK.id_pedido_origen AND VBPA_NIVEL1.cod_funcion_socio = 'L1' AND VBPA_NIVEL1.num_correlativo = 0 
    THEN REPLACE(VBPA_NIVEL1.id_cliente,'ITL-SAPS4-','') END cod_jerarquia_nivel_1,
    CASE WHEN VBPA_NIVEL2.id_documento=VBAK.id_pedido_origen AND VBPA_NIVEL2.cod_funcion_socio = 'L2' AND VBPA_NIVEL2.num_correlativo = 0 
    THEN REPLACE(VBPA_NIVEL2.id_cliente,'ITL-SAPS4-','') END cod_jerarquia_nivel_2,
    CASE WHEN VBPA_COMPLEMENTO.id_documento=REPLACE(VBAK.id_pedido,'PVE-SAPS4-','') AND VBPA_COMPLEMENTO.cod_funcion_socio = 'WE' 
    THEN REPLACE(VBPA_COMPLEMENTO.id_cliente,'ITL-SAPS4-','') END destinatario_mercancia,
    VBAK.id_pedido_origen documento_venta,
    VBAK.cod_clase_documento clase_documento_venta,
    CASE WHEN VBKD.id_pedido=VBAK.id_pedido AND VBKD.num_correlativo=0 THEN VBKD.cod_condicion_pago END condicion_pago,
    VBAK.cod_bloqueo_entrega bloqueo_entrega,
    VBAK.cod_condicion_expedicion condicion_expedicion,
    vbap_aux.num_correlativo posicion,
    REPLACE(vbap_aux.id_material,'MAT-SAPS4-','') material_historico,
    VBAK.fec_creacion fec_creacion,
    vbap_aux.cod_motivo_rechazo motivo_rechazo,
    vbap_aux.cod_centro centro,
    vbap_aux.cod_punto_expedicion puesto_expedicion,
    ---
    CASE 
    WHEN KNA1.id_interlocutor=VBAK.id_interlocutor AND KNA1.flg_cliente ='X' THEN KNA1.cod_pais END pais,
    CASE
    WHEN KNA1.id_interlocutor=VBAK.id_interlocutor AND KNA1.flg_cliente ='X' THEN KNA1.cod_region END region,
    CASE
    WHEN KNA1.id_interlocutor=VBAK.id_interlocutor AND KNA1.flg_cliente ='X' THEN KNA1.des_poblacion END poblacion,
    CASE
    WHEN KNA1.id_interlocutor=VBAK.id_interlocutor AND KNA1.flg_cliente ='X' THEN KNA1.des_distrito END distrito,
    ---
    CASE
    WHEN VBKD.id_pedido=VBAK.id_pedido AND VBKD.num_correlativo=0 THEN VBKD.cod_zona_cliente END zona_venta,
    ---
    VBAK.cod_oficina_venta oficina_venta,
    ---
    CASE
    WHEN VBKD.id_pedido=VBAK.id_pedido AND VBKD.num_correlativo=0 THEN VBKD.cod_grupo_cliente END grupo_cliente,
    VBAK.cod_segmento_cliente grupo_cliente_2,
    CASE
    WHEN VBKD.id_pedido=VBAK.id_pedido AND VBKD.num_correlativo=0 THEN VBKD.cod_grupo_condicion END grupo_condicion,
    CASE
    WHEN VBKD.id_pedido=VBAK.id_pedido AND VBKD.num_correlativo=0 THEN VBKD.cod_grupo_precio END grupo_precio,
    VBAK.cod_grupo_vendedor grupo_vendedor,
    CASE
    WHEN VBKD.id_pedido=VBAK.id_pedido AND VBKD.num_correlativo=0 THEN VBKD.cod_categoria_precio END lista_precio,
    vbap_aux.cod_marca marca,
    ---
    CASE
    WHEN MARA.id_material=vbap_aux.id_material THEN MARA.COD_DUENIO_MARCA END COD_DUENIO_MARCA,
    ---
    CASE
    WHEN MARA.id_material=vbap_aux.id_material THEN MARA.DES_DUENIO_MARCA END DES_DUENIO_MARCA,
    ---
    LEFT(vbap_aux.cod_jerarquia_material, 2) plataforma,
    LEFT(vbap_aux.cod_jerarquia_material, 4) subplataforma,
    LEFT(vbap_aux.cod_jerarquia_material, 7) categoia,
    LEFT(vbap_aux.cod_jerarquia_material, 10) familia,
    LEFT(vbap_aux.cod_jerarquia_material, 13) variedad,
    vbap_aux.cod_jerarquia_material presentacion,
    vbap_aux.cod_negocio negocio,
    vbap_aux.cod_subnegocio subnegocio,
    CASE
    WHEN MARA.id_material=vbap_aux.id_material THEN MARA.cod_tipo_material END tip_material,
    ---
    CASE
    WHEN VBPA.id_cliente is not null THEN REPLACE(VBPA.id_cliente,'ITL-SAPS4-','') ELSE REPLACE(VBPA_COMPLEMENTO_1.id_cliente,'ITL-SAPS4-','') 
    END territorio,
    ---
    LIPS.mnt_peso_entrega
  FROM LIPS
  INNER JOIN VBAK ON REPLACE(VBAK.id_pedido,'PVE-SAPS4-','')=LIPS.cod_documento_modelo 

  INNER JOIN VBAP vbap_aux ON VBAK.id_pedido=vbap_aux.id_pedido 
  and vbap_aux.num_correlativo=LIPS.num_correlativo_documento_modelo and 'PVE-' || 'SAPS4-' ||LIPS.cod_documento_modelo=vbap_aux.id_pedido

  LEFT JOIN VBPA VBPA_NIVEL1 ON VBPA_NIVEL1.id_documento=REPLACE(VBAK.id_pedido,'PVE-SAPS4-','')
  AND VBPA_NIVEL1.num_correlativo=0
  AND VBPA_NIVEL1.cod_funcion_socio='L1'

  LEFT JOIN VBPA VBPA_NIVEL2 ON VBPA_NIVEL2.id_documento=REPLACE(VBAK.id_pedido,'PVE-SAPS4-','')
  AND VBPA_NIVEL2.num_correlativo=0
  AND VBPA_NIVEL2.cod_funcion_socio='L2'


  LEFT JOIN VBPA VBPA_COMPLEMENTO ON VBPA_COMPLEMENTO.id_documento=REPLACE(VBAK.id_pedido,'PVE-SAPS4-','')
  AND VBPA_COMPLEMENTO.num_correlativo=0
  AND VBPA_COMPLEMENTO.cod_funcion_socio='WE'


  LEFT JOIN VBPA ON VBPA.id_documento=REPLACE(VBAK.id_pedido,'PVE-SAPS4-','')
  AND VBPA.num_correlativo=0
  AND VBPA.cod_funcion_socio='ZT'

  LEFT JOIN VBPA VBPA_COMPLEMENTO_1 ON VBPA_COMPLEMENTO_1.id_documento=REPLACE(vbap_aux.id_pedido,'PVE-SAPS4-','')
  AND VBPA_COMPLEMENTO_1.num_correlativo=vbap_aux.num_correlativo


  INNER JOIN MARA ON vbap_aux.id_material=MARA.id_material
  INNER JOIN KNA1 ON KNA1.id_interlocutor=VBAK.id_interlocutor and KNA1.des_origen="SAPS4"
  INNER JOIN VBKD ON VBKD.id_pedido=vbap_aux.id_pedido AND VBKD.num_correlativo=0 
;

---****************************************************************************************
/***GENERACION DE CAMPOS DEPENDIENTES TABLA PREVIA FINAL 1***/
CREATE OR REPLACE temp table UNIDAD_MEDIDA_VENTA_TABLA1 as 
  SELECT
    DISTINCT
    CASE 
    WHEN MVKE.cod_organizacion_venta=VBAK.cod_organizacion_venta 
    AND MVKE.cod_canal_distribucion=VBAK.cod_canal_distribucion 
    AND MVKE.id_material=VBAP.id_material 
    AND MVKE.cod_unidad_comercial IS NOT NULL THEN MVKE.cod_unidad_comercial 

    WHEN MVKE.cod_organizacion_venta=VBAK.cod_organizacion_venta 
    AND MVKE.cod_canal_distribucion=VBAK.cod_canal_distribucion 
    AND MVKE.id_material=VBAP.id_material 
    AND MVKE.cod_unidad_comercial IS NULL 
    AND MARA.id_material=VBAP.id_material THEN MARA.cod_unidad_medida_base END unidad_medida_venta, --REVISAR

    VBAK.id_pedido id_pedido_vbak, 
    VBAP.id_pedido id_pedido_vbap, 
    VBAP.num_correlativo num_correlativo_vbap, 
    VBAP.id_material
  FROM VBAK
  JOIN VBAP ON VBAP.id_pedido=VBAK.id_pedido
  JOIN MARA ON MARA.id_material=VBAP.id_material
  JOIN MVKE ON MVKE.id_material=VBAP.id_material
  AND MVKE.cod_organizacion_venta=VBAK.cod_organizacion_venta AND MVKE.cod_canal_distribucion=VBAK.cod_canal_distribucion
  --JOIN MARM ON MARM.id_material=VBAP.id_material
;


CREATE OR REPLACE temp table CANTIDAD AS
  SELECT 
    DISTINCT
    CASE 
    WHEN VBAP.cod_unidad_medida_venta=UNIDAD_MEDIDA_VENTA_TABLA1.unidad_medida_venta THEN VBAP.cnt_acumulada_venta ELSE 
    (CASE
      WHEN MARM.id_material=VBAP.id_material
      AND MARM.cod_unidad_medida=VBAP.cod_unidad_medida_venta 
      AND MARM_COMPLEMENTO.id_material=VBAP.id_material 
      AND MARM_COMPLEMENTO.cod_unidad_medida=UNIDAD_MEDIDA_VENTA_TABLA1.unidad_medida_venta
      THEN ((VBAP.cnt_acumulada_venta*MARM.num_numerador_conversion)/MARM.num_denominador_conversion)*MARM_COMPLEMENTO.num_denominador_conversion/MARM_COMPLEMENTO.num_numerador_conversion END 
    ) END cnt,
    VBAK.id_pedido id_pedido_vbak, --VBELN 'PVE-SAPS4-' prefijo
    VBAP.id_pedido id_pedido_vbap, --VBELN 'PVE-SAPS4-' prefijo
    VBAP.num_correlativo --POSNR
  FROM VBAK
  JOIN VBAP ON VBAP.id_pedido = VBAK.id_pedido

  JOIN MARM ON MARM.id_material=VBAP.id_material

  JOIN MARM_COMPLEMENTO ON MARM_COMPLEMENTO.id_material=VBAP.id_material

  JOIN UNIDAD_MEDIDA_VENTA_TABLA1 ON UNIDAD_MEDIDA_VENTA_TABLA1.id_pedido_vbak=VBAK.id_pedido
  AND UNIDAD_MEDIDA_VENTA_TABLA1.id_pedido_vbap=VBAP.id_pedido
  AND UNIDAD_MEDIDA_VENTA_TABLA1.num_correlativo_vbap=VBAP.num_correlativo
;


CREATE OR REPLACE temp table CANTIDAD_TABLA1 AS
  SELECT a.*
  FROM CANTIDAD a
  INNER JOIN 
  (SELECT id_pedido_vbap,num_correlativo,count(COALESCE(cnt, 0)) cantidad
  FROM CANTIDAD
  GROUP BY 1,2) b
  ON a.id_pedido_vbap = b.id_pedido_vbap
  AND a.num_correlativo = b.num_correlativo
  WHERE b.cantidad = 1 OR (b.cantidad = 2 AND a.cnt IS NOT NULL)
;


CREATE OR REPLACE temp table ENTREGA_TABLA1 AS 
  SELECT 
    DISTINCT
    CASE
    WHEN TVPOD.id_documento_tvpod_origen = (CASE WHEN LIPS.cod_documento_modelo=VBAK.id_pedido_origen 
                                            AND LIPS.num_correlativo_documento_modelo=VBAP.num_correlativo 
                                            THEN REPLACE(LIPS.id_documento_entrega,'DCE-SAPS4-','') END)
      AND TVPOD.num_correlativo = (CASE WHEN LIPS.cod_documento_modelo=VBAK.id_pedido_origen 
                                    AND LIPS.num_correlativo_documento_modelo=VBAP.num_correlativo 
                                    AND LIPS.cnt_entregada_unidad_venta != 0 THEN LIPS.num_correlativo END ) 
      THEN TVPOD.cod_motivo_desviacion 
    END motivo_desviacion,

    CASE 
    WHEN TVPOD.id_documento_tvpod_origen = (CASE WHEN LIPS.cod_documento_modelo=VBAK.id_pedido_origen 
                                            AND LIPS.num_correlativo_documento_modelo=VBAP.num_correlativo 
                                            THEN REPLACE(LIPS.id_documento_entrega,'DCE-SAPS4-','') END)
      AND TVPOD.num_correlativo = (CASE WHEN LIPS.cod_documento_modelo=VBAK.id_pedido_origen 
                                    AND LIPS.num_correlativo_documento_modelo=VBAP.num_correlativo 
                                    AND LIPS.cnt_entregada_unidad_venta != 0 THEN LIPS.num_correlativo END )
      AND TVPOD.fec_pedido IS NOT NULL THEN TVPOD.fec_pedido 
      WHEN TVPOD.id_documento_tvpod_origen = (CASE WHEN LIPS.cod_documento_modelo=VBAK.id_pedido_origen 
                                              AND LIPS.num_correlativo_documento_modelo=VBAP.num_correlativo 
                                              THEN REPLACE(LIPS.id_documento_entrega,'DCE-SAPS4-','') END )
      AND TVPOD.num_correlativo = (CASE WHEN LIPS.cod_documento_modelo=VBAK.id_pedido_origen 
                                    AND LIPS.num_correlativo_documento_modelo=VBAP.num_correlativo 
                                    AND LIPS.cnt_entregada_unidad_venta != 0 THEN LIPS.num_correlativo END ) 
      AND TVPOD.fec_pedido IS NULL THEN TVPOD.fec_notificacion 
    END fec_desviacion,

    CASE 
    WHEN LIPS.cod_documento_modelo  = VBAK.id_pedido_origen 
      AND LIPS.num_correlativo_documento_modelo = VBAP.num_correlativo 
    THEN REPLACE(LIPS.id_documento_entrega,'DCE-SAPS4-','') END entrega,

    CASE
    WHEN LIPS.cod_documento_modelo=VBAK.id_pedido_origen 
      AND LIPS.num_correlativo_documento_modelo=VBAP.num_correlativo 
      AND LIPS.cnt_entregada_unidad_venta != 0 THEN LIPS.num_correlativo 
    END posicion_entrega,

    VBAK.id_pedido id_pedido_vbak, --VBELN 'PVE-SAPS4-' prefijo
    LIPS.id_documento_entrega, --VBELN 'DCE-SAPS4-' prefijo
    LIPS.num_correlativo num_correlativo_lips, --POSNR-
    LIPS.num_correlativo_documento_modelo, --VGPOS
    LIPS.cod_documento_modelo, --VGBEL
    VBAP.id_pedido id_pedido_vbap, --VBELN 'PVE-SAPS4-' prefijo
    VBAP.num_correlativo num_correlativo_vbap--POSNR

  FROM VBAK
  JOIN VBAP ON VBAP.id_pedido=VBAK.id_pedido
  JOIN LIPS ON LIPS.cod_documento_modelo=REPLACE(VBAP.id_pedido,'PVE-SAPS4-','')
  AND LIPS.num_correlativo_documento_modelo=VBAP.num_correlativo
  AND LIPS.cnt_entregada_unidad_venta != 0

  -- AGREGAR DE TVPOD
  LEFT JOIN TVPOD ON TVPOD.id_documento_tvpod_origen=CASE WHEN REPLACE(LIPS.id_documento_entrega,'DCE-SAPS4-','') <> '' THEN REPLACE(LIPS.id_documento_entrega,'DCE-SAPS4-','') ELSE NULL END --REVISAR
  AND TVPOD.num_correlativo=LIPS.num_correlativo
;


CREATE OR REPLACE temp table CANTIDAD_ENTREGA AS
  SELECT
    DISTINCT
    CASE
      WHEN LIPS.cod_documento_modelo              =   VBAK.id_pedido_origen 
        AND LIPS.num_correlativo_documento_modelo =   VBAP.num_correlativo 
        AND LIPS.cnt_entregada_unidad_venta       !=  0 
        AND LIPS.cod_unidad_medida_venta          =   UNIDAD_MEDIDA_VENTA_TABLA1.unidad_medida_venta 
      THEN LIPS.cnt_entregada_unidad_venta
      WHEN LIPS.cod_documento_modelo              =   VBAK.id_pedido_origen 
        AND LIPS.num_correlativo_documento_modelo =   VBAP.num_correlativo 
        AND LIPS.cnt_entregada_unidad_venta       !=  0 
        AND LIPS.cod_unidad_medida_venta          !=  UNIDAD_MEDIDA_VENTA_TABLA1.unidad_medida_venta 
        AND MARM.id_material                       =  LIPS.id_material 
        AND MARM.cod_unidad_medida                 =  LIPS.cod_unidad_medida_venta 
        AND MARM_COMPLEMENTO.id_material           =  LIPS.id_material 
        AND MARM_COMPLEMENTO.cod_unidad_medida     =  UNIDAD_MEDIDA_VENTA_TABLA1.unidad_medida_venta
      THEN ((LIPS.cnt_entregada_unidad_venta * MARM.num_numerador_conversion) / MARM.num_denominador_conversion) * MARM_COMPLEMENTO.num_denominador_conversion / MARM_COMPLEMENTO.num_numerador_conversion
      ELSE NULL 
    END cnt_entrega,

    VBAK.id_pedido id_pedido_vbak, --VBELN 'PVE-SAPS4-' prefijo
    LIPS.id_documento_entrega, --VBELN 'DCE-SAPS4-' prefijo
    LIPS.num_correlativo num_correlativo_lips, --POSNR
    LIPS.num_correlativo_documento_modelo, --VGPOS
    LIPS.cod_documento_modelo, --VGBEL
    VBAP.id_pedido id_pedido_vbap, --VBELN 'PVE-SAPS4-' prefijo
    VBAP.num_correlativo num_correlativo_vbap--POSNR

  FROM VBAK
  JOIN VBAP ON VBAP.id_pedido=VBAK.id_pedido
  JOIN MARM ON MARM.id_material=VBAP.id_material
  JOIN MARM_COMPLEMENTO ON MARM_COMPLEMENTO.id_material=VBAP.id_material
  JOIN LIPS ON LIPS.cod_documento_modelo=REPLACE(VBAP.id_pedido,'PVE-SAPS4-','')
    AND LIPS.num_correlativo_documento_modelo=VBAP.num_correlativo
    AND LIPS.cnt_entregada_unidad_venta != 0
  JOIN UNIDAD_MEDIDA_VENTA_TABLA1 ON UNIDAD_MEDIDA_VENTA_TABLA1.id_pedido_vbak=VBAK.id_pedido
    AND UNIDAD_MEDIDA_VENTA_TABLA1.id_pedido_vbap=VBAP.id_pedido
    AND UNIDAD_MEDIDA_VENTA_TABLA1.num_correlativo_vbap=VBAP.num_correlativo
;


CREATE OR REPLACE temp table CANTIDAD_ENTREGA_TABLA1 AS
  SELECT 
    a.*
  FROM CANTIDAD_ENTREGA a
  INNER JOIN 
  (SELECT id_documento_entrega,num_correlativo_lips,count(COALESCE(cnt_entrega, 0)) cantidad
  FROM CANTIDAD_ENTREGA
  GROUP BY 1,2) b
  ON a.id_documento_entrega = b.id_documento_entrega
  AND a.num_correlativo_lips = b.num_correlativo_lips
  WHERE b.cantidad = 1 
  OR (b.cantidad = 2 AND a.cnt_entrega IS NOT NULL)
;


CREATE OR REPLACE temp table CANTIDAD_TOTAL_DESVIACION_TABLA1 AS
  SELECT
    DISTINCT
    CASE 
      WHEN TVPOD.id_documento_tvpod_origen          =     ENTREGA_TABLA1.entrega 
        AND TVPOD.num_correlativo                   =     ENTREGA_TABLA1.posicion_entrega 
        AND TVPOD.cnt_desviacion                    <>    0 
        AND TVPOD.cod_unidad_medida_venta           =     UNIDAD_MEDIDA_VENTA_TABLA1.unidad_medida_venta 
      THEN TVPOD.cnt_desviacion 
      WHEN TVPOD.id_documento_tvpod_origen          =     ENTREGA_TABLA1.entrega 
        AND TVPOD.num_correlativo                   =     ENTREGA_TABLA1.posicion_entrega 
        AND TVPOD.cnt_desviacion                    <>    0 
        AND TVPOD.cod_unidad_medida_venta           !=    UNIDAD_MEDIDA_VENTA_TABLA1.unidad_medida_venta
        AND MARM.id_material                        =     TVPOD.id_material 
        AND MARM.cod_unidad_medida                  =     TVPOD.cod_unidad_medida_venta 
        AND MARM_COMPLEMENTO.id_material            =     TVPOD.id_material 
        AND MARM_COMPLEMENTO.cod_unidad_medida      =     TVPOD.cod_unidad_medida_base 
      THEN ((TVPOD.cnt_desviacion * MARM.num_numerador_conversion) / MARM.num_denominador_conversion) * MARM_COMPLEMENTO.num_denominador_conversion / MARM_COMPLEMENTO.num_numerador_conversion
      ELSE NULL
    END cnt_desviacion_total,

    VBAK.id_pedido id_pedido_vbak, --VBELN 'PVE-SAPS4-' prefijo
    LIPS.id_documento_entrega, --VBELN 'DCE-SAPS4-' prefijo
    LIPS.num_correlativo num_correlativo_lips, --POSNR
    LIPS.num_correlativo_documento_modelo, --VGPOS
    LIPS.cod_documento_modelo, --VGBEL
    VBAP.id_pedido id_pedido_vbap, --VBELN 'PVE-SAPS4-' prefijo
    VBAP.num_correlativo num_correlativo_vbap--POSNR

  FROM LIPS

  LEFT JOIN VBAP ON LIPS.cod_documento_modelo=REPLACE(VBAP.id_pedido,'PVE-SAPS4-','')
  AND LIPS.num_correlativo_documento_modelo=VBAP.num_correlativo
  AND LIPS.cnt_entregada_unidad_venta <> 0

  LEFT JOIN TVPOD ON TVPOD.id_documento_tvpod_origen = CASE WHEN REPLACE(LIPS.id_documento_entrega,'DCE-SAPS4-','') <> '' THEN REPLACE(LIPS.id_documento_entrega,'DCE-SAPS4-','') ELSE NULL END --REVISAR
    AND TVPOD.num_correlativo=LIPS.num_correlativo

  JOIN VBAK ON VBAP.id_pedido=VBAK.id_pedido
  JOIN MARM ON MARM.id_material=VBAP.id_material
  --AND MARM.cod_unidad_medida=VBAP.cod_unidad_medida_base 
  JOIN MARM_COMPLEMENTO ON MARM_COMPLEMENTO.id_material=VBAP.id_material
  JOIN UNIDAD_MEDIDA_VENTA_TABLA1 ON UNIDAD_MEDIDA_VENTA_TABLA1.id_pedido_vbak=VBAK.id_pedido
    AND UNIDAD_MEDIDA_VENTA_TABLA1.id_pedido_vbap=VBAP.id_pedido
    AND UNIDAD_MEDIDA_VENTA_TABLA1.num_correlativo_vbap=VBAP.num_correlativo

  JOIN ENTREGA_TABLA1 ON ENTREGA_TABLA1.id_pedido_vbak=VBAK.id_pedido
    AND ENTREGA_TABLA1.id_documento_entrega=LIPS.id_documento_entrega
    AND ENTREGA_TABLA1.num_correlativo_lips=LIPS.num_correlativo
    AND ENTREGA_TABLA1.num_correlativo_documento_modelo=LIPS.num_correlativo_documento_modelo
    AND ENTREGA_TABLA1.cod_documento_modelo=LIPS.cod_documento_modelo
    AND ENTREGA_TABLA1.id_pedido_vbap=VBAP.id_pedido
    AND ENTREGA_TABLA1.num_correlativo_vbap=VBAP.num_correlativo
;


CREATE OR REPLACE temp table CANTIDAD_TOTAL_DESVIACION_1 AS
  SELECT 
    a.*
  FROM CANTIDAD_TOTAL_DESVIACION_TABLA1 a
  INNER JOIN 
  (SELECT id_documento_entrega,num_correlativo_lips,count(COALESCE(cnt_desviacion_total, 0)) cantidad
  FROM CANTIDAD_TOTAL_DESVIACION_TABLA1
  GROUP BY 1,2) b
  ON a.id_documento_entrega = b.id_documento_entrega
  AND a.num_correlativo_lips = b.num_correlativo_lips
  WHERE b.cantidad = 1 OR (b.cantidad = 2 AND a.cnt_desviacion_total IS NOT NULL)
;


--LLAVES id_documento_entrega,num_correlativo_lips /no hay duplicados
--103990 / 104204
CREATE OR REPLACE temp table FACTURA_TABLA1 AS
  SELECT
    DISTINCT
    CASE 
      WHEN TVPOD.id_documento_tvpod_origen=ENTREGA_TABLA1.entrega 
      AND TVPOD.num_correlativo=ENTREGA_TABLA1.posicion_entrega 
      THEN TVPOD.cod_motivo_desviacion 
    END motivo_desviacion,

    CASE 
      WHEN TVPOD.id_documento_tvpod_origen=ENTREGA_TABLA1.entrega 
      AND TVPOD.num_correlativo=ENTREGA_TABLA1.posicion_entrega 
      THEN TVPOD.fec_pedido 
    END fec_desviacion,

    CASE 
      WHEN VBRP.cod_documento_modelo=ENTREGA_TABLA1.entrega 
      AND VBRP.num_correlativo_documento_modelo=ENTREGA_TABLA1.posicion_entrega 
      AND VBRP.cnt_material <> 0 
      THEN REPLACE(VBRP.id_documento, 'DOC-SAPS4-','') 
    END factura,

    CASE 
      WHEN VBRP.cod_documento_modelo=ENTREGA_TABLA1.entrega 
      AND VBRP.num_correlativo_documento_modelo=ENTREGA_TABLA1.posicion_entrega 
      AND VBRP.cnt_material <> 0 
      THEN VBRP.num_correlativo 
    END posicion_factura,

    VBAK.id_pedido id_pedido_vbak, --VBELN 'PVE-SAPS4-' prefijo
    LIPS.id_documento_entrega, --VBELN 'DCE-SAPS4-' prefijo
    LIPS.num_correlativo num_correlativo_lips, --POSNR
    LIPS.num_correlativo_documento_modelo, --VGPOS
    LIPS.cod_documento_modelo, --VGBEL
    VBAP.id_pedido id_pedido_vbap, --VBELN 'PVE-SAPS4-' prefijo
    VBAP.num_correlativo num_correlativo_vbap--POSNR

  FROM LIPS
  LEFT JOIN VBAP ON LIPS.cod_documento_modelo     =   REPLACE(VBAP.id_pedido,'PVE-SAPS4-','')
    AND LIPS.num_correlativo_documento_modelo     =   VBAP.num_correlativo
    AND LIPS.cnt_entregada_unidad_venta           <>  0

  LEFT JOIN VBRP ON VBRP.cod_documento_modelo = REPLACE(LIPS.id_documento_entrega,'DCE-SAPS4-','')
    AND VBRP.num_correlativo_documento_modelo = LIPS.num_correlativo

  LEFT JOIN TVPOD ON TVPOD.id_documento_tvpod_origen=CASE WHEN REPLACE(LIPS.id_documento_entrega,'DCE-SAPS4-','') <> '' THEN REPLACE(LIPS.id_documento_entrega,'DCE-SAPS4-','') ELSE NULL END --REVISAR
    AND TVPOD.num_correlativo = LIPS.num_correlativo

  JOIN VBRK ON VBRK.id_documento_origen = REPLACE(VBRP.id_documento,'DOC-SAPS4-','')

  JOIN VBAK ON VBAP.id_pedido = VBAK.id_pedido

  JOIN ENTREGA_TABLA1 ON ENTREGA_TABLA1.id_pedido_vbak        =   VBAK.id_pedido
    AND ENTREGA_TABLA1.id_documento_entrega                   =   LIPS.id_documento_entrega
    AND ENTREGA_TABLA1.num_correlativo_lips                   =   LIPS.num_correlativo
    AND ENTREGA_TABLA1.num_correlativo_documento_modelo       =   LIPS.num_correlativo_documento_modelo
    AND ENTREGA_TABLA1.cod_documento_modelo                   =   LIPS.cod_documento_modelo
    AND ENTREGA_TABLA1.id_pedido_vbap                         =   VBAP.id_pedido
    AND ENTREGA_TABLA1.num_correlativo_vbap                   =   VBAP.num_correlativo
;


CREATE OR REPLACE temp table CANTIDAD_FACTURADA AS
  SELECT
    DISTINCT
    CASE 
      WHEN VBRP.cod_documento_modelo              = ENTREGA_TABLA1.entrega 
        AND VBRP.num_correlativo_documento_modelo = ENTREGA_TABLA1.posicion_entrega 
        AND VBRP.cnt_material                     <> 0 
        AND VBRP.cod_unidad_medida_venta          = UNIDAD_MEDIDA_VENTA_TABLA1.unidad_medida_venta 
      THEN VBRP.cnt_material 
      WHEN VBRP.cod_documento_modelo              =   ENTREGA_TABLA1.entrega 
        AND VBRP.num_correlativo_documento_modelo =   ENTREGA_TABLA1.posicion_entrega 
        AND VBRP.cnt_material                     <>  0 
        AND VBRP.cod_unidad_medida_venta          !=  UNIDAD_MEDIDA_VENTA_TABLA1.unidad_medida_venta
        AND MARM.id_material                       =  VBRP.id_material 
        AND MARM.cod_unidad_medida                 =  VBRP.cod_unidad_medida_venta 
        AND MARM_COMPLEMENTO.id_material           =  VBRP.id_material 
        AND MARM_COMPLEMENTO.cod_unidad_medida     =  UNIDAD_MEDIDA_VENTA_TABLA1.unidad_medida_venta
      THEN ((VBRP.cnt_material * MARM.num_numerador_conversion) / MARM.num_denominador_conversion) * MARM_COMPLEMENTO.num_denominador_conversion / MARM_COMPLEMENTO.num_numerador_conversion  
      ELSE NULL 
    END cnt_facturada,

    VBAK.id_pedido id_pedido_vbak, --VBELN 'PVE-SAPS4-' prefijo
    LIPS.id_documento_entrega, --VBELN 'DCE-SAPS4-' prefijo
    LIPS.num_correlativo num_correlativo_lips, --POSNR
    LIPS.num_correlativo_documento_modelo, --VGPOS
    LIPS.cod_documento_modelo, --VGBEL
    VBAP.id_pedido id_pedido_vbap, --VBELN 'PVE-SAPS4-' prefijo
    VBAP.num_correlativo num_correlativo_vbap,--POSNR

    VBRP.mnt_peso_factura,
    VBRP.valor_factura

  FROM LIPS

  LEFT JOIN VBAP ON LIPS.cod_documento_modelo = REPLACE(VBAP.id_pedido,'PVE-SAPS4-','')
    AND LIPS.num_correlativo_documento_modelo = VBAP.num_correlativo
    AND LIPS.cnt_entregada_unidad_venta       <> 0

  LEFT JOIN VBRP ON VBRP.cod_documento_modelo   = REPLACE(LIPS.id_documento_entrega,'DCE-SAPS4-','')
    AND VBRP.num_correlativo_documento_modelo   = LIPS.num_correlativo

  JOIN VBAK ON VBAP.id_pedido = VBAK.id_pedido
  JOIN MARM ON MARM.id_material = VBAP.id_material
  --AND MARM.cod_unidad_medida=VBAP.cod_unidad_medida_base 
  JOIN MARM_COMPLEMENTO ON MARM_COMPLEMENTO.id_material = VBAP.id_material
  --AND MARM_COMPLEMENTO.cod_unidad_medida=VBAP.cod_unidad_medida_base 

  JOIN VBRK ON VBRK.id_documento_origen = REPLACE(VBRP.id_documento,'DOC-SAPS4-','')

  JOIN UNIDAD_MEDIDA_VENTA_TABLA1 ON UNIDAD_MEDIDA_VENTA_TABLA1.id_pedido_vbak  = VBAK.id_pedido
    AND UNIDAD_MEDIDA_VENTA_TABLA1.id_pedido_vbap       = VBAP.id_pedido
    AND UNIDAD_MEDIDA_VENTA_TABLA1.num_correlativo_vbap = VBAP.num_correlativo

  JOIN ENTREGA_TABLA1 ON ENTREGA_TABLA1.id_pedido_vbak    =   VBAK.id_pedido
    AND ENTREGA_TABLA1.id_documento_entrega               =   LIPS.id_documento_entrega
    AND ENTREGA_TABLA1.num_correlativo_lips               =   LIPS.num_correlativo
    AND ENTREGA_TABLA1.num_correlativo_documento_modelo   =   LIPS.num_correlativo_documento_modelo
    AND ENTREGA_TABLA1.cod_documento_modelo               =   LIPS.cod_documento_modelo
    AND ENTREGA_TABLA1.id_pedido_vbap                     =   VBAP.id_pedido
    AND ENTREGA_TABLA1.num_correlativo_vbap               =   VBAP.num_correlativo
;


CREATE OR REPLACE temp table CANTIDAD_FACTURADA_TABLA1 AS
  SELECT 
    a.*
  FROM CANTIDAD_FACTURADA a
  INNER JOIN 
  (SELECT id_documento_entrega,num_correlativo_lips,count(COALESCE(cnt_facturada, 0)) cantidad
  FROM CANTIDAD_FACTURADA
  GROUP BY 1,2) b
  ON a.id_documento_entrega = b.id_documento_entrega
  AND a.num_correlativo_lips = b.num_correlativo_lips
  WHERE b.cantidad = 1 OR (b.cantidad = 2 AND a.cnt_facturada IS NOT NULL)
;


CREATE OR REPLACE temp table DETALLE_FACTURA_TABLA1 AS
  SELECT 
    DISTINCT
    CASE 
    WHEN VBRK.id_documento_origen=FACTURA_TABLA1.factura THEN VBRK.cod_clase_documento END clase_factura,
    CASE 
    WHEN VBRK.id_documento_origen=FACTURA_TABLA1.factura THEN VBRK.fec_documento END fec_factura,

    CASE 
      WHEN FACTURA_TABLA1.factura IS NOT NULL 
        AND VBPA.id_documento       = FACTURA_TABLA1.factura 
        AND VBPA.cod_funcion_socio  = 'ZT' 
      THEN REPLACE(VBPA.id_cliente,'ITL-SAPS4-','')
      WHEN FACTURA_TABLA1.factura IS NULL 
        AND VBPA_COMPLEMENTO.id_documento       = REPLACE(VBAK.id_pedido,'PVE-SAPS4-','') 
        AND VBPA_COMPLEMENTO.cod_funcion_socio  = 'ZT' 
      THEN REPLACE(VBPA_COMPLEMENTO.id_cliente,'ITL-SAPS4-','') 
    END territorio,

    CASE 
      WHEN VBAP3.cod_documento_modelo               = REPLACE(VBRP.id_documento,'DOC-SAPS4-','') 
        AND  VBAP3.num_correlativo_documento_modelo = VBRP.num_correlativo 
      THEN REPLACE(VBAP3.id_pedido,'PVE-SAPS4-','') 
    END documento_rechazo,

    CASE
      WHEN REPLACE(VBAP3.id_pedido,'PVE-SAPS4-','') = (CASE WHEN VBAP3.cod_documento_modelo = REPLACE(VBRP.id_documento,'DOC-SAPS4-','') 
                                                      AND VBAP3.num_correlativo_documento_modelo=VBRP.num_correlativo 
                                                      THEN REPLACE(VBAP3.id_pedido,'PVE-SAPS4-','') END) 
      AND VBAP3.cnt_acumulada_venta <> 0 
      THEN VBAP3.num_correlativo 
    END posicion_rechazo,

    VBAK.id_pedido id_pedido_vbak, --VBELN 'PVE-SAPS4-' prefijo
    LIPS.id_documento_entrega, --VBELN 'DCE-SAPS4-' prefijo
    LIPS.num_correlativo num_correlativo_lips, --POSNR
    LIPS.num_correlativo_documento_modelo, --VGPOS
    LIPS.cod_documento_modelo, --VGBEL
    VBAP3.id_pedido id_pedido_vbap, --VBELN 'PVE-SAPS4-' prefijo
    VBAP3.num_correlativo num_correlativo_vbap,--POSNR
    FACTURA_TABLA1.factura,
    FACTURA_TABLA1.posicion_factura

  FROM LIPS LIPS

  LEFT JOIN VBRP VBRP ON VBRP.cod_documento_modelo  = REPLACE(LIPS.id_documento_entrega,'DCE-SAPS4-','')
    AND VBRP.num_correlativo_documento_modelo       = LIPS.num_correlativo

  LEFT JOIN VBAP3 VBAP3 ON VBAP3.cod_documento_modelo = REPLACE(VBRP.id_documento,'DOC-SAPS4-','')
    AND VBAP3.num_correlativo_documento_modelo        = VBRP.num_correlativo

  JOIN VBRK VBRK ON VBRK.id_documento_origen  = REPLACE(VBRP.id_documento,'DOC-SAPS4-','')

  LEFT JOIN VBAK VBAK ON REPLACE(VBAK.id_pedido,'PVE-SAPS4-','')  = LIPS.cod_documento_modelo

  LEFT JOIN VBPA VBPA ON VBPA.id_documento  = VBRK.id_documento_origen
    AND VBPA.cod_funcion_socio              = 'ZT'

  LEFT JOIN VBPA VBPA_COMPLEMENTO ON VBPA_COMPLEMENTO.id_documento  = REPLACE(VBAK.id_pedido,'PVE-SAPS4-','')
    AND VBPA_COMPLEMENTO.cod_funcion_socio                          = 'ZT'

  JOIN FACTURA_TABLA1 FACTURA_TABLA1 ON FACTURA_TABLA1.id_pedido_vbak = VBAK.id_pedido
    AND FACTURA_TABLA1.id_documento_entrega                           = LIPS.id_documento_entrega
    AND FACTURA_TABLA1.num_correlativo_lips                           = LIPS.num_correlativo
    AND FACTURA_TABLA1.num_correlativo_documento_modelo               = LIPS.num_correlativo_documento_modelo
    AND FACTURA_TABLA1.cod_documento_modelo                           = LIPS.cod_documento_modelo
;


CREATE OR REPLACE temp table RECHAZO AS
  SELECT
    DISTINCT
    CASE 
    WHEN VBAK3.id_pedido_origen = DETALLE_FACTURA_TABLA1.documento_rechazo THEN VBAK3.fec_creacion END fec_rechazo_factura,

    CASE 
      WHEN REPLACE(VBAP3.id_pedido,'PVE-SAPS4-','') = DETALLE_FACTURA_TABLA1.documento_rechazo 
        AND VBAP3.cnt_acumulada_venta <> 0 
      THEN VBAP3.num_correlativo 
    END posicion_rechazo,

    CASE
    WHEN VBAK3.id_pedido  = VBAP3.id_pedido THEN VBAK3.cod_motivo_pedido END cod_motivo_rechazo_del_rechazo,

    VBAK3.id_pedido id_pedido_vbak, --VBELN 'PVE-SAPS4-' prefijo
    LIPS.id_documento_entrega, --VBELN 'DCE-SAPS4-' prefijo
    LIPS.num_correlativo num_correlativo_lips, --POSNR
    LIPS.num_correlativo_documento_modelo, --VGPOS
    LIPS.cod_documento_modelo, --VGBEL
    VBAP3.id_pedido id_pedido_vbap, --VBELN 'PVE-SAPS4-' prefijo
    VBAP3.num_correlativo num_correlativo_vbap,--POSNR
    DETALLE_FACTURA_TABLA1.factura,
    DETALLE_FACTURA_TABLA1.posicion_factura,
    DETALLE_FACTURA_TABLA1.documento_rechazo

  FROM LIPS

  LEFT JOIN VBRP ON VBRP.cod_documento_modelo = REPLACE(LIPS.id_documento_entrega,'DCE-SAPS4-','')
    AND VBRP.num_correlativo_documento_modelo = LIPS.num_correlativo

  LEFT JOIN VBAP3 ON VBAP3.cod_documento_modelo = REPLACE(VBRP.id_documento,'DOC-SAPS4-','')
    AND VBAP3.num_correlativo_documento_modelo  = VBRP.num_correlativo

  LEFT JOIN VBRK ON VBRK.id_documento_origen  = REPLACE(VBRP.id_documento,'DOC-SAPS4-','')

  LEFT JOIN VBAK3 ON VBAK3.id_pedido  = VBAP3.id_pedido

  JOIN DETALLE_FACTURA_TABLA1 ON DETALLE_FACTURA_TABLA1.id_documento_entrega  = LIPS.id_documento_entrega
    AND DETALLE_FACTURA_TABLA1.num_correlativo_lips                           = LIPS.num_correlativo
    AND DETALLE_FACTURA_TABLA1.num_correlativo_documento_modelo               = LIPS.num_correlativo_documento_modelo
    AND DETALLE_FACTURA_TABLA1.cod_documento_modelo                           = LIPS.cod_documento_modelo
;


CREATE OR REPLACE temp table RECHAZO_TABLA1 AS
  SELECT DISTINCT 
    a.*
  FROM RECHAZO a
  INNER JOIN 
  (SELECT id_documento_entrega,num_correlativo_lips,count(COALESCE(posicion_rechazo, 0)) posicion_rechazo
  FROM RECHAZO
  GROUP BY 1,2) b
  ON a.id_documento_entrega = b.id_documento_entrega
  AND a.num_correlativo_lips = b.num_correlativo_lips
  WHERE b.posicion_rechazo = 1 OR (b.posicion_rechazo >1 AND a.posicion_rechazo IS NOT NULL)
;


CREATE OR REPLACE temp table CANTIDAD_RECHAZO AS
  SELECT
    DISTINCT
    CASE
      WHEN REPLACE(VBAP3.id_pedido,'PVE-SAPS4-','')         =   DETALLE_FACTURA_TABLA1.documento_rechazo 
        AND VBAP3.cnt_acumulada_venta                       <>  0 
        AND UNIDAD_MEDIDA_VENTA_TABLA1.unidad_medida_venta  =   VBAP3.cod_unidad_medida_venta 
      THEN VBAP3.cnt_acumulada_venta 
      WHEN REPLACE(VBAP3.id_pedido,'PVE-SAPS4-','')         =   DETALLE_FACTURA_TABLA1.documento_rechazo 
        AND VBAP3.cnt_acumulada_venta                       <>  0 
        AND UNIDAD_MEDIDA_VENTA_TABLA1.unidad_medida_venta  !=  VBAP3.cod_unidad_medida_venta
        AND MARM.id_material                                 =  VBAP3.id_material 
        AND MARM.cod_unidad_medida                           =  VBAP3.cod_unidad_medida_venta 
        AND MARM_COMPLEMENTO.id_material                     =  VBAP3.id_material 
        AND MARM_COMPLEMENTO.cod_unidad_medida               =  UNIDAD_MEDIDA_VENTA_TABLA1.unidad_medida_venta 
      THEN ((VBAP3.cnt_acumulada_venta * MARM.num_numerador_conversion) / MARM.num_denominador_conversion) * MARM_COMPLEMENTO.num_denominador_conversion / MARM_COMPLEMENTO.num_numerador_conversion 
      ELSE NULL 
    END cnt_rechazo,

    VBAK3.id_pedido id_pedido_vbak, --VBELN 'PVE-SAPS4-' prefijo
    LIPS.id_documento_entrega, --VBELN 'DCE-SAPS4-' prefijo
    LIPS.num_correlativo num_correlativo_lips, --POSNR
    LIPS.num_correlativo_documento_modelo, --VGPOS
    LIPS.cod_documento_modelo, --VGBEL
    VBAP3.id_pedido id_pedido_vbap, --VBELN 'PVE-SAPS4-' prefijo
    VBAP3.num_correlativo num_correlativo_vbap,--POSNR
    DETALLE_FACTURA_TABLA1.factura,
    DETALLE_FACTURA_TABLA1.posicion_factura,
    DETALLE_FACTURA_TABLA1.posicion_rechazo,
    DETALLE_FACTURA_TABLA1.documento_rechazo,

    VBAP3.mnt_peso_rechazo,
    VBAP3.valor_rechazo

  FROM LIPS
  ---EL SIGUIENTE INNER ES POR MIENTRAS HASTA QUE ENCUENTREN LA FALLA
  INNER JOIN RECHAZO_TABLA1 
  ON LIPS.id_documento_entrega  = RECHAZO_TABLA1.id_documento_entrega
    AND LIPS.num_correlativo    = RECHAZO_TABLA1.num_correlativo_lips

  LEFT JOIN VBRP ON VBRP.cod_documento_modelo   =   REPLACE(LIPS.id_documento_entrega,'DCE-SAPS4-','')
    AND VBRP.num_correlativo_documento_modelo   =   LIPS.num_correlativo

  LEFT JOIN VBAP3 ON VBAP3.cod_documento_modelo   =   REPLACE(VBRP.id_documento,'DOC-SAPS4-','')
    AND VBAP3.num_correlativo_documento_modelo    =   VBRP.num_correlativo

  INNER JOIN VBRK ON VBRK.id_documento_origen = REPLACE(VBRP.id_documento,'DOC-SAPS4-','')

  LEFT JOIN VBAK3 ON VBAK3.id_pedido  = VBAP3.id_pedido

  LEFT JOIN MARM ON MARM.id_material  = VBAP3.id_material

  LEFT JOIN MARM_COMPLEMENTO ON MARM_COMPLEMENTO.id_material  = VBAP3.id_material

  LEFT JOIN DETALLE_FACTURA_TABLA1 ON DETALLE_FACTURA_TABLA1.id_documento_entrega = LIPS.id_documento_entrega
    AND DETALLE_FACTURA_TABLA1.num_correlativo_lips                               = LIPS.num_correlativo
    AND DETALLE_FACTURA_TABLA1.num_correlativo_documento_modelo                   = LIPS.num_correlativo_documento_modelo
    AND DETALLE_FACTURA_TABLA1.cod_documento_modelo                               = LIPS.cod_documento_modelo

  LEFT JOIN UNIDAD_MEDIDA_VENTA_TABLA1 ON UNIDAD_MEDIDA_VENTA_TABLA1.id_material  = VBAP3.id_material
;


CREATE OR REPLACE temp table CANTIDAD_RECHAZO_TABLA1 AS 
  SELECT 
    a.*
  FROM CANTIDAD_RECHAZO a
  INNER JOIN 
  (SELECT id_documento_entrega,num_correlativo_lips,count(COALESCE(cnt_rechazo, 0)) cantidad
  FROM CANTIDAD_RECHAZO
  GROUP BY 1,2) b
  ON a.id_documento_entrega = b.id_documento_entrega
    AND a.num_correlativo_lips = b.num_correlativo_lips
  WHERE b.cantidad = 1 
    OR (b.cantidad >1 AND a.cnt_rechazo IS NOT NULL)
;

--##########################################################################################################
--################################# CREACION DE LA TABLA PREVIA FINAL 1 ####################################
--##########################################################################################################
CREATE OR REPLACE temp table TABLA_PREVIA_FINAL_1 AS 
  SELECT 
    INDEPENDIENTE_TABLA1.*,
    CANTIDAD_FACTURADA_TABLA1.mnt_peso_factura,
    CANTIDAD_FACTURADA_TABLA1.valor_factura,
    ENTREGA_TABLA1.entrega,
    ENTREGA_TABLA1.posicion_entrega,
    CANTIDAD_ENTREGA_TABLA1.cnt_entrega,
    UNIDAD_MEDIDA_VENTA_TABLA1.unidad_medida_venta,
    CANTIDAD_TABLA1.cnt,
    ENTREGA_TABLA1.motivo_desviacion,
    ENTREGA_TABLA1.fec_desviacion,
    FACTURA_TABLA1.factura,
    FACTURA_TABLA1.posicion_factura,
    DETALLE_FACTURA_TABLA1.clase_factura,
    DETALLE_FACTURA_TABLA1.fec_factura,
    --DETALLE_FACTURA_TABLA1.territorio,
    CANTIDAD_FACTURADA_TABLA1.cnt_facturada,
    DETALLE_FACTURA_TABLA1.documento_rechazo,
    RECHAZO_TABLA1.fec_rechazo_factura,
    RECHAZO_TABLA1.posicion_rechazo,
    RECHAZO_TABLA1.cod_motivo_rechazo_del_rechazo,
    CANTIDAD_RECHAZO_TABLA1.cnt_rechazo,
    CANTIDAD_RECHAZO_TABLA1.mnt_peso_rechazo,
    CANTIDAD_RECHAZO_TABLA1.valor_rechazo,
    CANTIDAD_TOTAL_DESVIACION_1.cnt_desviacion_total,
    1 AS flg
  FROM INDEPENDIENTE_TABLA1

  JOIN ENTREGA_TABLA1
  ON INDEPENDIENTE_TABLA1.id_documento_entrega    = ENTREGA_TABLA1.id_documento_entrega
    AND INDEPENDIENTE_TABLA1.num_correlativo_lips = ENTREGA_TABLA1.num_correlativo_lips

  LEFT JOIN UNIDAD_MEDIDA_VENTA_TABLA1
  ON UNIDAD_MEDIDA_VENTA_TABLA1.id_pedido_vbap          = ENTREGA_TABLA1.id_pedido_vbap
    AND UNIDAD_MEDIDA_VENTA_TABLA1.num_correlativo_vbap = ENTREGA_TABLA1.num_correlativo_vbap

  LEFT JOIN CANTIDAD_TABLA1
  ON CANTIDAD_TABLA1.id_pedido_vbap     = ENTREGA_TABLA1.id_pedido_vbap
    AND CANTIDAD_TABLA1.num_correlativo = ENTREGA_TABLA1.num_correlativo_vbap

  JOIN CANTIDAD_ENTREGA_TABLA1
  ON INDEPENDIENTE_TABLA1.id_documento_entrega    = CANTIDAD_ENTREGA_TABLA1.id_documento_entrega
    AND INDEPENDIENTE_TABLA1.num_correlativo_lips = CANTIDAD_ENTREGA_TABLA1.num_correlativo_lips

  LEFT JOIN CANTIDAD_TOTAL_DESVIACION_1
  ON CANTIDAD_TOTAL_DESVIACION_1.id_documento_entrega     = ENTREGA_TABLA1.id_documento_entrega
    AND CANTIDAD_TOTAL_DESVIACION_1.num_correlativo_lips  = ENTREGA_TABLA1.num_correlativo_lips

  LEFT JOIN FACTURA_TABLA1
  ON FACTURA_TABLA1.id_documento_entrega    = ENTREGA_TABLA1.id_documento_entrega
    AND FACTURA_TABLA1.num_correlativo_lips = ENTREGA_TABLA1.num_correlativo_lips

  LEFT JOIN CANTIDAD_FACTURADA_TABLA1
  ON CANTIDAD_FACTURADA_TABLA1.id_documento_entrega     = ENTREGA_TABLA1.id_documento_entrega
    AND CANTIDAD_FACTURADA_TABLA1.num_correlativo_lips  = ENTREGA_TABLA1.num_correlativo_lips

  LEFT JOIN DETALLE_FACTURA_TABLA1
  ON DETALLE_FACTURA_TABLA1.id_documento_entrega      = ENTREGA_TABLA1.id_documento_entrega
    AND DETALLE_FACTURA_TABLA1.num_correlativo_lips   = ENTREGA_TABLA1.num_correlativo_lips

  LEFT JOIN RECHAZO_TABLA1
  ON RECHAZO_TABLA1.id_documento_entrega    = ENTREGA_TABLA1.id_documento_entrega
    AND RECHAZO_TABLA1.num_correlativo_lips = ENTREGA_TABLA1.num_correlativo_lips
    AND RECHAZO_TABLA1.posicion_rechazo     = DETALLE_FACTURA_TABLA1.posicion_rechazo
    AND RECHAZO_TABLA1.documento_rechazo    = DETALLE_FACTURA_TABLA1.documento_rechazo

  LEFT JOIN CANTIDAD_RECHAZO_TABLA1
  ON CANTIDAD_RECHAZO_TABLA1.id_documento_entrega     = ENTREGA_TABLA1.id_documento_entrega
    AND CANTIDAD_RECHAZO_TABLA1.num_correlativo_lips  = ENTREGA_TABLA1.num_correlativo_lips
    AND CANTIDAD_RECHAZO_TABLA1.posicion_rechazo      = DETALLE_FACTURA_TABLA1.posicion_rechazo
    AND CANTIDAD_RECHAZO_TABLA1.documento_rechazo     = DETALLE_FACTURA_TABLA1.documento_rechazo
;


--#################################### GENERACION DE CAMPOS DEPENDIENTES TABLA PREVIA FINAL 2 ############################
CREATE OR REPLACE temp table UNIDAD_MEDIDA_VENTA_TABLA2 as 
  SELECT
    DISTINCT
    CASE 
      WHEN MVKE.cod_organizacion_venta  = VBAK.cod_organizacion_venta 
        AND MVKE.cod_canal_distribucion = VBAK.cod_canal_distribucion 
        AND MVKE.id_material            = VBAP1.id_material 
        AND MVKE.cod_unidad_comercial IS NOT NULL 
      THEN MVKE.cod_unidad_comercial 
      WHEN MVKE.cod_organizacion_venta  = VBAK.cod_organizacion_venta 
        AND MVKE.cod_canal_distribucion = VBAK.cod_canal_distribucion 
        AND MVKE.id_material            = VBAP1.id_material 
        AND MVKE.cod_unidad_comercial IS NULL 
        AND MARA1.id_material           = VBAP1.id_material 
      THEN MARA1.cod_unidad_medida_base 
    END unidad_medida_venta, --REVISAR

    VBAK.id_pedido id_pedido_vbak, --VBELN 'PVE-SAPS4-' prefijo
    VBAP1.id_pedido id_pedido_vbap, --VBELN 'PVE-SAPS4-' prefijo
    VBAP1.num_correlativo num_correlativo_vbap, --POSNR
    VBAP1.id_material

  FROM VBAK
  JOIN VBAP1 ON VBAP1.id_pedido     = VBAK.id_pedido
  JOIN MARA1 ON MARA1.id_material   = VBAP1.id_material
  JOIN MVKE ON MVKE.id_material     = VBAP1.id_material
    AND MVKE.cod_organizacion_venta = VBAK.cod_organizacion_venta 
    AND MVKE.cod_canal_distribucion = VBAK.cod_canal_distribucion
--JOIN MARM ON MARM.id_material=VBAP.id_material
;


CREATE OR REPLACE temp table CANTIDAD_2 AS
  SELECT 
    DISTINCT
    CASE 
      WHEN VBAP1.cod_unidad_medida_venta=UNIDAD_MEDIDA_VENTA_TABLA2.unidad_medida_venta THEN VBAP1.cnt_acumulada_venta 
    ELSE 
    (
      CASE
      WHEN MARM1.id_material        = VBAP1.id_material
        AND MARM1.cod_unidad_medida = VBAP1.cod_unidad_medida_venta 
        AND MARM1_COMPLEMENTO.id_material = VBAP1.id_material 
        AND MARM1_COMPLEMENTO.cod_unidad_medida = UNIDAD_MEDIDA_VENTA_TABLA2.unidad_medida_venta
      THEN ((VBAP1.cnt_acumulada_venta * MARM1.num_numerador_conversion) / MARM1.num_denominador_conversion) * MARM1_COMPLEMENTO.num_denominador_conversion / MARM1_COMPLEMENTO.num_numerador_conversion END 
    ) 
    END cnt,

    VBAK.id_pedido id_pedido_vbak, --VBELN 'PVE-SAPS4-' prefijo
    VBAP1.id_pedido id_pedido_vbap, --VBELN 'PVE-SAPS4-' prefijo
    VBAP1.num_correlativo --POSNR

  FROM VBAK
  JOIN VBAP1 ON VBAP1.id_pedido = VBAK.id_pedido

  JOIN MARM1 ON MARM1.id_material = VBAP1.id_material

  JOIN MARM1_COMPLEMENTO ON MARM1_COMPLEMENTO.id_material = VBAP1.id_material

  JOIN UNIDAD_MEDIDA_VENTA_TABLA2 ON UNIDAD_MEDIDA_VENTA_TABLA2.id_pedido_vbak  = VBAK.id_pedido
    AND UNIDAD_MEDIDA_VENTA_TABLA2.id_pedido_vbap       = VBAP1.id_pedido
    AND UNIDAD_MEDIDA_VENTA_TABLA2.num_correlativo_vbap = VBAP1.num_correlativo
;


CREATE OR REPLACE temp table CANTIDAD_TABLA2 AS
  SELECT 
    a.*
  FROM CANTIDAD_2 a
  INNER JOIN 
  (SELECT id_pedido_vbap,num_correlativo,count(COALESCE(cnt, 0)) cantidad
  FROM CANTIDAD_2
  GROUP BY 1,2) b
  ON a.id_pedido_vbap = b.id_pedido_vbap
  AND a.num_correlativo = b.num_correlativo
  WHERE b.cantidad = 1 OR (b.cantidad = 2 AND a.cnt IS NOT NULL)
;


--############################################################################################################
--################### GENERACION DE CAMPOS INDEPENDIENTES TABLA PREVIA FINAL 2 ###############################
--############################################################################################################
CREATE OR REPLACE temp table INDEPENDIENTE_TABLA2 AS 
  SELECT
    DISTINCT 
    VBAP1.mnt_peso_neto,
    VBAP1.cod_unidad_medida_peso,
    VBAP1.mnt_neto_pedido,
    VBAP1.cod_moneda,

    VBAP1.id_pedido, --CONSULTAR EL MOTIVO DE PORQUE SE AÑADIÓ

    VBAK.cod_sociedad_factura sociedad,
    VBAK.cod_organizacion_venta organizacion_venta,
    VBAK.cod_canal_distribucion canal,
    VBAK.cod_sector_comercial sector,
    REPLACE(VBAK.id_interlocutor,'ITL-SAPS4-','') cliente,

    CASE WHEN VBPA_NIVEL1.id_documento  = VBAK.id_pedido_origen 
      AND VBPA_NIVEL1.cod_funcion_socio = 'L1' 
      AND VBPA_NIVEL1.num_correlativo   = 0 
      THEN REPLACE(VBPA_NIVEL1.id_cliente,'ITL-SAPS4-','') 
    END cod_jerarquia_nivel_1,

    CASE WHEN VBPA_NIVEL2.id_documento  = VBAK.id_pedido_origen 
      AND VBPA_NIVEL2.cod_funcion_socio   = 'L2' 
      AND VBPA_NIVEL2.num_correlativo     = 0 
      THEN REPLACE(VBPA_NIVEL2.id_cliente,'ITL-SAPS4-','') 
    END cod_jerarquia_nivel_2,

    CASE WHEN VBPA_COMPLEMENTO_1.id_documento   = REPLACE(VBAK.id_pedido,'PVE-SAPS4-','') 
      AND VBPA_COMPLEMENTO_1.cod_funcion_socio  = 'WE' 
      THEN REPLACE(VBPA_COMPLEMENTO_1.id_cliente,'ITL-SAPS4-','') 
    ELSE NULL 
    END destinatario_mercancia,

    VBAK.id_pedido_origen documento_venta,
    VBAK.cod_clase_documento clase_documento_venta,

    CASE WHEN VBKD.id_pedido  = VBAK.id_pedido AND VBKD.num_correlativo = 0 THEN VBKD.cod_condicion_pago END condicion_pago,

    VBAK.cod_bloqueo_entrega bloqueo_entrega,
    VBAK.cod_condicion_expedicion condicion_expedicion,

    VBAP1.num_correlativo posicion,
    REPLACE(VBAP1.id_material,'MAT-SAPS4-','') material_historico,

    /*************EN EL SELECT FINAL AQUI DEBE IR MATERIAL ACTUAL, UNIDAD MEDIDA VENTA, CANTIDAD***************/
    VBAK.fec_creacion fec_creacion,
    VBAP1.cod_motivo_rechazo motivo_rechazo,

    /*************EN EL SELECT FINAL AQUI DEBE IR FEC_RECHAZO***************/
    VBAP1.cod_centro centro,
    VBAP1.cod_punto_expedicion puesto_expedicion,

    CASE 
    WHEN KNA1.id_interlocutor = VBAK.id_interlocutor AND KNA1.flg_cliente ='X' THEN KNA1.cod_pais END pais,
    CASE
    WHEN KNA1.id_interlocutor = VBAK.id_interlocutor AND KNA1.flg_cliente ='X' THEN KNA1.cod_region END region,
    CASE
    WHEN KNA1.id_interlocutor = VBAK.id_interlocutor AND KNA1.flg_cliente ='X' THEN KNA1.des_poblacion END poblacion,
    CASE
    WHEN KNA1.id_interlocutor = VBAK.id_interlocutor AND KNA1.flg_cliente ='X' THEN KNA1.des_distrito END distrito,

    CASE
    WHEN VBKD.id_pedido = VBAK.id_pedido AND VBKD.num_correlativo = 0 THEN VBKD.cod_zona_cliente END zona_venta,

    VBAK.cod_oficina_venta oficina_venta,

    CASE
    WHEN VBKD.id_pedido = VBAK.id_pedido AND VBKD.num_correlativo = 0 THEN VBKD.cod_grupo_cliente END grupo_cliente,

    VBAK.cod_segmento_cliente grupo_cliente_2,

    CASE
    WHEN VBKD.id_pedido = VBAK.id_pedido AND VBKD.num_correlativo = 0 THEN VBKD.cod_grupo_condicion END grupo_condicion,
    CASE
    WHEN VBKD.id_pedido = VBAK.id_pedido AND VBKD.num_correlativo = 0 THEN VBKD.cod_grupo_precio END grupo_precio,

    VBAK.cod_grupo_vendedor grupo_vendedor,

    CASE
    WHEN VBKD.id_pedido = VBAK.id_pedido AND VBKD.num_correlativo = 0 THEN VBKD.cod_categoria_precio END lista_precio,

    VBAP1.cod_marca marca,

    CASE
    WHEN MARA1.id_material  = VBAP1.id_material THEN MARA1.COD_DUENIO_MARCA END COD_DUENIO_MARCA,

    CASE
    WHEN MARA1.id_material  = VBAP1.id_material THEN MARA1.DES_DUENIO_MARCA END DES_DUENIO_MARCA,

    LEFT(VBAP1.cod_jerarquia_material, 2) plataforma,
    LEFT(VBAP1.cod_jerarquia_material, 4) subplataforma,
    LEFT(VBAP1.cod_jerarquia_material, 7) categoia,
    LEFT(VBAP1.cod_jerarquia_material, 10) familia,
    LEFT(VBAP1.cod_jerarquia_material, 13) variedad,
    VBAP1.cod_jerarquia_material presentacion,
    VBAP1.cod_negocio negocio,
    VBAP1.cod_subnegocio subnegocio,
    --MEJORAR
    CASE
    WHEN MARA1.id_material=VBAP1.id_material THEN MARA1.cod_tipo_material END tip_material,

    CASE
      WHEN VBPA_COMPLEMENTO.id_documento        = REPLACE(VBAK.id_pedido,'PVE-SAPS4-','') 
        AND VBPA_COMPLEMENTO.cod_funcion_socio  = 'ZT' 
      THEN REPLACE(VBPA_COMPLEMENTO.id_cliente,'ITL-SAPS4-','') 
    END territorio,

    null as entrega,
    null as posicion_entrega,
    null as cnt_entrega,
    null as motivo_desviacion,
    null as fec_desviacion,
    null as factura,
    null as posicion_factura,
    null as cnt_facturada,
    null as clase_factura,
    null as fec_factura,
    null as documento_rechazo,
    null as fec_rechazo_factura,
    null as posicion_rechazo,
    null as cod_motivo_rechazo_del_rechazo,
    null as cnt_rechazo

  FROM VBAK
  --JOIN VBAP ON VBAK.id_pedido=VBAP.id_pedido
  JOIN VBPA VBPA_NIVEL1 ON VBPA_NIVEL1.id_documento = REPLACE(VBAK.id_pedido,'PVE-SAPS4-','')
    AND VBPA_NIVEL1.cod_funcion_socio               = 'L1'

  JOIN VBPA VBPA_NIVEL2 ON VBPA_NIVEL2.id_documento = REPLACE(VBAK.id_pedido,'PVE-SAPS4-','')
    AND VBPA_NIVEL2.cod_funcion_socio               = 'L2'

  JOIN VBPA VBPA_COMPLEMENTO_1 ON VBPA_COMPLEMENTO_1.id_documento = REPLACE(VBAK.id_pedido,'PVE-SAPS4-','')
    AND VBPA_COMPLEMENTO_1.cod_funcion_socio                      = 'WE'

  JOIN VBPA VBPA_COMPLEMENTO ON VBPA_COMPLEMENTO.id_documento = REPLACE(VBAK.id_pedido,'PVE-SAPS4-','')
    AND VBPA_COMPLEMENTO.cod_funcion_socio                    = 'ZT'

  JOIN VBAP1 ON VBAK.id_pedido  = VBAP1.id_pedido
  JOIN MARA1 ON VBAP1.id_material = MARA1.id_material
  JOIN KNA1 ON KNA1.id_interlocutor = VBAK.id_interlocutor

  JOIN VBKD ON VBKD.id_pedido = VBAK.id_pedido 
    AND VBKD.num_correlativo  = 0
;


CREATE OR REPLACE temp table TABLA_PREVIA_FINAL_2 AS 
  SELECT 
  DISTINCT
    a.mnt_peso_neto,
    a.cod_unidad_medida_peso,
    a.mnt_neto_pedido,
    a.cod_moneda,
    SAFE_CAST(NULL AS STRING) AS id_documento_entrega,
    SAFE_CAST(NULL AS INT64) AS num_correlativo_lips,
    SAFE_CAST(NULL AS INT64) AS num_correlativo_documento_modelo,
    SAFE_CAST(NULL AS STRING) AS cod_documento_modelo,
    a.sociedad,
    a.organizacion_venta,
    a.canal,
    a.sector,
    a.cliente,
    a.cod_jerarquia_nivel_1,
    a.cod_jerarquia_nivel_2,
    a.destinatario_mercancia,
    a.documento_venta,
    a.clase_documento_venta,
    a.condicion_pago,
    a.bloqueo_entrega,
    a.condicion_expedicion,
    a.posicion,
    a.material_historico,
    a.fec_creacion,
    a.motivo_rechazo,
    a.centro,
    a.puesto_expedicion,
    a.pais,
    a.region,
    a.poblacion,
    a.distrito,
    a.zona_venta,
    a.oficina_venta,
    a.grupo_cliente,
    a.grupo_cliente_2,
    a.grupo_condicion,
    a.grupo_precio,
    a.grupo_vendedor,
    a.lista_precio,
    a.marca,
    a.COD_DUENIO_MARCA,
    a.DES_DUENIO_MARCA,
    a.plataforma,
    a.subplataforma,
    a.categoia,
    a.familia,
    a.variedad,
    a.presentacion,
    a.negocio,
    a.subnegocio,
    a.tip_material,
    a.territorio,
    SAFE_CAST(NULL AS INT64) AS mnt_peso_entrega,
    SAFE_CAST(NULL AS INT64) AS mnt_peso_factura,
    SAFE_CAST(NULL AS INT64) AS valor_factura,
    SAFE_CAST(a.entrega AS STRING) AS entrega, 
    a.posicion_entrega,
    a.cnt_entrega,
    b.unidad_medida_venta,
    c.cnt,
    SAFE_CAST(a.motivo_desviacion AS STRING) AS motivo_desviacion,
    DATE(TIMESTAMP_SECONDS(a.fec_desviacion)) AS fec_desviacion,
    SAFE_CAST(a.factura AS STRING) AS factura,
    a.posicion_factura,
    SAFE_CAST(a.clase_factura AS STRING) AS clase_factura,
    DATE(TIMESTAMP_SECONDS(a.fec_factura)) AS fec_factura,
    SAFE_CAST(a.cnt_facturada AS NUMERIC) AS cnt_facturada,
    SAFE_CAST(a.documento_rechazo AS STRING) AS documento_rechazo,
    DATE(TIMESTAMP_SECONDS(a.fec_rechazo_factura)) AS fec_rechazo_factura,
    a.posicion_rechazo,
    SAFE_CAST(a.cod_motivo_rechazo_del_rechazo AS STRING) AS cod_motivo_rechazo_del_rechazo,
    a.cnt_rechazo,
    SAFE_CAST(NULL AS INT64) AS mnt_peso_rechazo,
    SAFE_CAST(NULL AS INT64) AS valor_rechazo,
    SAFE_CAST(NULL AS INT64) AS cnt_desviacion_total,
    2 AS flg
  FROM INDEPENDIENTE_TABLA2 as a
  LEFT JOIN UNIDAD_MEDIDA_VENTA_TABLA2 b
  ON a.id_pedido  = b.id_pedido_vbak
  AND a.posicion  = b.num_correlativo_vbap

  LEFT JOIN CANTIDAD_TABLA2 c
  ON a.id_pedido  = c.id_pedido_vbap
  AND a.posicion  = c.num_correlativo
;


CREATE OR REPLACE temp table INDEPENDIENTE3 AS 
  SELECT
    DISTINCT
    VBAP2.mnt_peso_neto,
    VBAP2.cod_unidad_medida_peso,
    VBAP2.mnt_neto_pedido,
    VBAP2.cod_moneda,

    LIPS1.id_documento_entrega,
    LIPS1.num_correlativo num_correlativo_lips,
    LIPS1.num_correlativo_documento_modelo,
    LIPS1.cod_documento_modelo,

    VBAK.cod_sociedad_factura sociedad,
    VBAK.cod_organizacion_venta organizacion_venta,
    VBAK.cod_canal_distribucion canal,
    VBAK.cod_sector_comercial sector,
    REPLACE(VBAK.id_interlocutor,'ITL-SAPS4-','') cliente,

    CASE WHEN VBPA2_NIVEL1.id_documento   = VBAK.id_pedido_origen 
      AND VBPA2_NIVEL1.cod_funcion_socio  = 'L1' 
      AND VBPA2_NIVEL1.num_correlativo    =  0 
      THEN REPLACE(VBPA2_NIVEL1.id_cliente,'ITL-SAPS4-','') 
    END cod_jerarquia_nivel_1,

    CASE WHEN VBPA2_NIVEL2.id_documento   = VBAK.id_pedido_origen 
      AND VBPA2_NIVEL2.cod_funcion_socio  = 'L2' 
      AND VBPA2_NIVEL2.num_correlativo    = 0 
      THEN REPLACE(VBPA2_NIVEL2.id_cliente,'ITL-SAPS4-','') 
    END cod_jerarquia_nivel_2,

    CASE WHEN VBPA2_COMPLEMENTO.id_documento    = REPLACE(VBAK.id_pedido,'PVE-SAPS4-','') 
      AND VBPA2_COMPLEMENTO.cod_funcion_socio   = 'WE' 
      THEN REPLACE(VBPA2_COMPLEMENTO.id_cliente,'ITL-SAPS4-','') 
      ELSE NULL 
    END destinatario_mercancia,

    VBAK.id_pedido_origen documento_venta,
    VBAK.cod_clase_documento clase_documento_venta,

    CASE WHEN VBKD.id_pedido  = VBAK.id_pedido AND VBKD.num_correlativo =  0 THEN VBKD.cod_condicion_pago END condicion_pago,

    VBAK.cod_bloqueo_entrega bloqueo_entrega,
    VBAK.cod_condicion_expedicion condicion_expedicion,
    VBAP2.num_correlativo posicion,
    REPLACE(VBAP2.id_material,'MAT-SAPS4-','') material_historico,

    /*************EN EL SELECT FINAL AQUI DEBE IR MATERIAL ACTUAL, UNIDAD MEDIDA VENTA, CANTIDAD***************/
    VBAK.fec_creacion fec_creacion,
    VBAP2.cod_motivo_rechazo motivo_rechazo,

    /*************EN EL SELECT FINAL AQUI DEBE IR FEC_RECHAZO***************/
    VBAP2.cod_centro centro,
    VBAP2.cod_punto_expedicion puesto_expedicion,

    CASE 
    WHEN KNA1.id_interlocutor = VBAK.id_interlocutor AND KNA1.flg_cliente ='X' THEN KNA1.cod_pais END pais,
    CASE
    WHEN KNA1.id_interlocutor = VBAK.id_interlocutor AND KNA1.flg_cliente ='X' THEN KNA1.cod_region END region,
    CASE
    WHEN KNA1.id_interlocutor = VBAK.id_interlocutor AND KNA1.flg_cliente ='X' THEN KNA1.des_poblacion END poblacion,
    CASE
    WHEN KNA1.id_interlocutor = VBAK.id_interlocutor AND KNA1.flg_cliente ='X' THEN KNA1.des_distrito END distrito,

    CASE
    WHEN VBKD.id_pedido = VBAK.id_pedido AND VBKD.num_correlativo = 0 THEN VBKD.cod_zona_cliente END zona_venta,

    VBAK.cod_oficina_venta oficina_venta,

    CASE
    WHEN VBKD.id_pedido = VBAK.id_pedido AND VBKD.num_correlativo = 0 THEN VBKD.cod_grupo_cliente END grupo_cliente,

    VBAK.cod_segmento_cliente grupo_cliente_2,

    CASE
    WHEN VBKD.id_pedido = VBAK.id_pedido AND VBKD.num_correlativo = 0 THEN VBKD.cod_grupo_condicion END grupo_condicion,
    CASE
    WHEN VBKD.id_pedido = VBAK.id_pedido AND VBKD.num_correlativo = 0 THEN VBKD.cod_grupo_precio END grupo_precio,
    
    VBAK.cod_grupo_vendedor grupo_vendedor,

    CASE
    WHEN VBKD.id_pedido = VBAK.id_pedido AND VBKD.num_correlativo = 0 THEN VBKD.cod_categoria_precio END lista_precio,

    VBAP2.cod_marca marca,

    CASE
    WHEN MARA2.id_material  = VBAP2.id_material THEN MARA2.COD_DUENIO_MARCA END COD_DUENIO_MARCA,

    CASE
    WHEN MARA2.id_material  = VBAP2.id_material THEN MARA2.DES_DUENIO_MARCA END DES_DUENIO_MARCA,

    LEFT(VBAP2.cod_jerarquia_material, 2) plataforma,
    LEFT(VBAP2.cod_jerarquia_material, 4) subplataforma,
    LEFT(VBAP2.cod_jerarquia_material, 7) categoia,
    LEFT(VBAP2.cod_jerarquia_material, 10) familia,
    LEFT(VBAP2.cod_jerarquia_material, 13) variedad,
    VBAP2.cod_jerarquia_material presentacion,
    VBAP2.cod_negocio negocio,
    VBAP2.cod_subnegocio subnegocio,
    CASE
    WHEN MARA2.id_material  = VBAP2.id_material THEN MARA2.cod_tipo_material END tip_material,

    CASE
      WHEN VBPA2.id_cliente is not null THEN REPLACE(VBPA2.id_cliente,'ITL-SAPS4-','') 
      ELSE REPLACE(VBPA2_COMPLEMENTO_1.id_cliente,'ITL-SAPS4-','') 
    END territorio,
    LIPS1.mnt_peso_entrega

  FROM LIPS1
  JOIN VBAK ON REPLACE(VBAK.id_pedido,'PVE-SAPS4-','')  = LIPS1.cod_documento_modelo

  JOIN VBAP2 ON VBAK.id_pedido                          = VBAP2.id_pedido
    AND VBAP2.num_correlativo                           = LIPS1.num_correlativo_documento_modelo 
    AND 'PVE-' || 'SAPS4-' ||LIPS1.cod_documento_modelo = VBAP2.id_pedido

  LEFT JOIN VBPA2 VBPA2_NIVEL1 ON VBPA2_NIVEL1.id_documento = REPLACE(VBAK.id_pedido,'PVE-SAPS4-','')
    AND VBPA2_NIVEL1.num_correlativo                        = 0
    AND VBPA2_NIVEL1.cod_funcion_socio                      = 'L1'

  LEFT JOIN VBPA2 VBPA2_NIVEL2 ON VBPA2_NIVEL2.id_documento = REPLACE(VBAK.id_pedido,'PVE-SAPS4-','')
    AND VBPA2_NIVEL2.num_correlativo                        = 0
    AND VBPA2_NIVEL2.cod_funcion_socio                      = 'L2'

  LEFT JOIN VBPA2 VBPA2_COMPLEMENTO ON VBPA2_COMPLEMENTO.id_documento = REPLACE(VBAK.id_pedido,'PVE-SAPS4-','')
    AND VBPA2_COMPLEMENTO.num_correlativo                             = 0
    AND VBPA2_COMPLEMENTO.cod_funcion_socio                           = 'WE'

  LEFT JOIN VBPA2 ON VBPA2.id_documento = REPLACE(VBAK.id_pedido,'PVE-SAPS4-','')
    AND VBPA2.num_correlativo           = 0
    AND VBPA2.cod_funcion_socio         = 'ZT'

  LEFT JOIN VBPA2 VBPA2_COMPLEMENTO_1 ON VBPA2_COMPLEMENTO_1.id_documento = REPLACE(VBAP2.id_pedido,'PVE-SAPS4-','')
    AND VBPA2_COMPLEMENTO_1.num_correlativo                               = VBAP2.num_correlativo

  JOIN MARA2 ON VBAP2.id_material = MARA2.id_material

  JOIN KNA1 ON KNA1.id_interlocutor = VBAK.id_interlocutor
    AND KNA1.des_origen             = 'SAPS4'

  JOIN VBKD ON VBKD.id_pedido = VBAK.id_pedido
    AND VBKD.num_correlativo  = 0
;


/**********GENERACION DE CAMPOS DEPENDIENTES TABLA PREVIA FINAL 3**********/
CREATE OR REPLACE TEMP TABLE UNIDAD_MEDIDA_VENTA3 AS
  SELECT
  DISTINCT
    CASE 
      WHEN MVKE.cod_organizacion_venta  = VBAK.cod_organizacion_venta 
        AND MVKE.cod_canal_distribucion = VBAK.cod_canal_distribucion 
        AND MVKE.id_material            = VBAP2.id_material 
        AND MVKE.cod_unidad_comercial IS NOT NULL 
      THEN MVKE.cod_unidad_comercial 
      WHEN MVKE.cod_organizacion_venta  = VBAK.cod_organizacion_venta 
        AND MVKE.cod_canal_distribucion = VBAK.cod_canal_distribucion 
        AND MVKE.id_material            = VBAP2.id_material 
        AND MVKE.cod_unidad_comercial IS NULL 
        AND MARA2.id_material           = VBAP2.id_material 
      THEN MARA2.cod_unidad_medida_base 
    END unidad_medida_venta,

    VBAK.id_pedido id_pedido_vbak, --VBELN 'PVE-SAPS4-' prefijo
    VBAP2.id_pedido id_pedido_vbap, --VBELN 'PVE-SAPS4-' prefijo
    VBAP2.num_correlativo num_correlativo_vbap, --POSNR
    VBAP2.id_material
  FROM VBAK
  JOIN VBAP2 ON VBAP2.id_pedido   = VBAK.id_pedido
  JOIN MARA2 ON MARA2.id_material = VBAP2.id_material
  JOIN MVKE ON MVKE.id_material     = VBAP2.id_material
    AND MVKE.cod_organizacion_venta = VBAK.cod_organizacion_venta 
    AND MVKE.cod_canal_distribucion = VBAK.cod_canal_distribucion
;


CREATE OR REPLACE TEMP TABLE CANTIDAD_COMPLETO3 AS
  SELECT 
  DISTINCT
    CASE 
      WHEN VBAP2.cod_unidad_medida_venta  = UNIDAD_MEDIDA_VENTA3.unidad_medida_venta 
      THEN VBAP2.cnt_acumulada_venta 
      ELSE 
      (
        CASE
        WHEN MARM2.id_material  = VBAP2.id_material
          AND MARM2.cod_unidad_medida             = VBAP2.cod_unidad_medida_venta 
          AND MARM2_COMPLEMENTO.id_material       = VBAP2.id_material 
          AND MARM2_COMPLEMENTO.cod_unidad_medida = UNIDAD_MEDIDA_VENTA3.unidad_medida_venta 
        THEN ((VBAP2.cnt_acumulada_venta * MARM2.num_numerador_conversion) / MARM2.num_denominador_conversion) * MARM2_COMPLEMENTO.num_denominador_conversion / MARM2_COMPLEMENTO.num_numerador_conversion END 
      ) 
    END cnt,
    
    VBAK.id_pedido id_pedido_vbak, 
    VBAP2.id_pedido id_pedido_vbap, 
    VBAP2.num_correlativo 

  FROM VBAK
  JOIN VBAP2 ON VBAP2.id_pedido   = VBAK.id_pedido
  JOIN MARM2 ON MARM2.id_material = VBAP2.id_material
    AND MARM2.cod_unidad_medida   = VBAP2.cod_unidad_medida_venta

  JOIN MARM2_COMPLEMENTO ON MARM2_COMPLEMENTO.id_material = VBAP2.id_material

  JOIN UNIDAD_MEDIDA_VENTA3 ON UNIDAD_MEDIDA_VENTA3.id_pedido_vbak  = VBAK.id_pedido 
    AND UNIDAD_MEDIDA_VENTA3.id_pedido_vbap                         = VBAP2.id_pedido
    AND UNIDAD_MEDIDA_VENTA3.num_correlativo_vbap                   = VBAP2.num_correlativo
;


CREATE OR REPLACE TEMP TABLE CANTIDAD3 AS
  SELECT 
    a.*
  FROM CANTIDAD_COMPLETO3 a
  INNER JOIN 
  (SELECT id_pedido_vbap,num_correlativo,count(COALESCE(cnt, 0)) cantidad
  FROM CANTIDAD_COMPLETO3
  GROUP BY 1,2) b
  ON a.id_pedido_vbap = b.id_pedido_vbap
  AND a.num_correlativo = b.num_correlativo
  WHERE b.cantidad = 1 
  OR (b.cantidad = 2 AND a.cnt IS NOT NULL)
;


CREATE OR REPLACE TEMP TABLE ENTREGA3 AS 
  SELECT 
  DISTINCT
    CASE 
      WHEN TVPOD1.id_documento_tvpod_origen = (CASE WHEN LIPS1.cod_documento_modelo       = VBAK.id_pedido_origen 
                                              AND LIPS1.num_correlativo_documento_modelo  = VBAP2.num_correlativo 
                                              THEN REPLACE(LIPS1.id_documento_entrega,'DCE-SAPS4-','') END)
        AND TVPOD1.num_correlativo  = (CASE WHEN LIPS1.cod_documento_modelo         = VBAK.id_pedido_origen 
                                        AND LIPS1.num_correlativo_documento_modelo  = VBAP2.num_correlativo 
                                        AND LIPS1.cnt_entregada_unidad_venta        <> 0 
                                        THEN LIPS1.num_correlativo END) 
      THEN TVPOD1.cod_motivo_desviacion 
    END motivo_desviacion,

    CASE 
      WHEN TVPOD1.id_documento_tvpod_origen=(CASE WHEN LIPS1.cod_documento_modelo = VBAK.id_pedido_origen 
                                              AND LIPS1.num_correlativo_documento_modelo  = VBAP2.num_correlativo 
                                              THEN REPLACE(LIPS1.id_documento_entrega,'DCE-SAPS4-','') END)
          AND TVPOD1.num_correlativo  = (CASE WHEN LIPS1.cod_documento_modelo = VBAK.id_pedido_origen 
                                          AND LIPS1.num_correlativo_documento_modelo  = VBAP2.num_correlativo 
                                          AND LIPS1.cnt_entregada_unidad_venta <> 0 
                                          THEN LIPS1.num_correlativo END) 
          AND TVPOD1.fec_pedido IS NOT NULL 
      THEN TVPOD1.fec_pedido
      WHEN TVPOD1.id_documento_tvpod_origen = (CASE WHEN LIPS1.cod_documento_modelo = VBAK.id_pedido_origen 
                                                AND LIPS1.num_correlativo_documento_modelo  = VBAP2.num_correlativo 
                                                THEN REPLACE(LIPS1.id_documento_entrega,'DCE-SAPS4-','') END) 
          AND TVPOD1.num_correlativo=(CASE WHEN LIPS1.cod_documento_modelo  = VBAK.id_pedido_origen 
                                        AND LIPS1.num_correlativo_documento_modelo  = VBAP2.num_correlativo 
                                        AND LIPS1.cnt_entregada_unidad_venta <> 0 
                                        THEN LIPS1.num_correlativo END) 
          AND TVPOD1.fec_pedido IS NULL 
      THEN TVPOD1.fec_notificacion 
    END fec_desviacion,

    CASE 
    WHEN LIPS1.cod_documento_modelo = VBAK.id_pedido_origen 
    AND LIPS1.num_correlativo_documento_modelo  = VBAP2.num_correlativo 
    THEN REPLACE(LIPS1.id_documento_entrega,'DCE-SAPS4-','') END entrega,

    CASE
      WHEN LIPS1.cod_documento_modelo = VBAK.id_pedido_origen 
        AND LIPS1.num_correlativo_documento_modelo  = VBAP2.num_correlativo 
        AND LIPS1.cnt_entregada_unidad_venta        <> 0 
      THEN LIPS1.num_correlativo 
    END posicion_entrega,

    VBAK.id_pedido id_pedido_vbak, --VBELN 'PVE-SAPS4-' prefijo
    LIPS1.id_documento_entrega, --VBELN 'DCE-SAPS4-' prefijo
    LIPS1.num_correlativo num_correlativo_lips, --POSNR
    LIPS1.num_correlativo_documento_modelo, --VGPOS
    LIPS1.cod_documento_modelo, --VGBEL
    VBAP2.id_pedido id_pedido_vbap, --VBELN 'PVE-SAPS4-' prefijo
    VBAP2.num_correlativo num_correlativo_vbap--POSNR

  FROM VBAK
  JOIN VBAP2 ON VBAP2.id_pedido =  VBAK.id_pedido
  JOIN LIPS1 ON LIPS1.cod_documento_modelo      = REPLACE(VBAP2.id_pedido,'PVE-SAPS4-','')
    AND LIPS1.num_correlativo_documento_modelo  = VBAP2.num_correlativo
    AND LIPS1.cnt_entregada_unidad_venta       != 0

  LEFT JOIN TVPOD1 ON TVPOD1.id_documento_tvpod_origen  = CASE WHEN REPLACE(LIPS1.id_documento_entrega,'DCE-SAPS4-','') <> '' THEN REPLACE(LIPS1.id_documento_entrega,'DCE-SAPS4-','') ELSE NULL END --REVISAR
    AND TVPOD1.num_correlativo  = LIPS1.num_correlativo
;


CREATE OR REPLACE TEMP TABLE CANTIDAD_ENTREGA_COMPLETA3 AS
  SELECT
    DISTINCT
    CASE
      WHEN LIPS1.cod_documento_modelo               =   VBAK.id_pedido_origen 
        AND LIPS1.num_correlativo_documento_modelo  =   VBAP2.num_correlativo 
        AND LIPS1.cnt_entregada_unidad_venta        <>  0 
        AND LIPS1.cod_unidad_medida_venta           =   UNIDAD_MEDIDA_VENTA3.unidad_medida_venta 
      THEN LIPS1.cnt_entregada_unidad_venta
      WHEN LIPS1.cod_documento_modelo               =   VBAK.id_pedido_origen 
        AND LIPS1.num_correlativo_documento_modelo  =   VBAP2.num_correlativo 
        AND LIPS1.cnt_entregada_unidad_venta        <>  0 
        AND LIPS1.cod_unidad_medida_venta           !=  UNIDAD_MEDIDA_VENTA3.unidad_medida_venta 
        AND MARM2.id_material                       =   LIPS1.id_material 
        AND MARM2.cod_unidad_medida                 =   LIPS1.cod_unidad_medida_venta 
        AND MARM2_COMPLEMENTO.id_material           =   LIPS1.id_material 
        AND MARM2_COMPLEMENTO.cod_unidad_medida     =   UNIDAD_MEDIDA_VENTA3.unidad_medida_venta
      THEN ((LIPS1.cnt_entregada_unidad_venta*MARM2.num_numerador_conversion) / MARM2.num_denominador_conversion) * MARM2_COMPLEMENTO.num_denominador_conversion / MARM2_COMPLEMENTO.num_numerador_conversion
      ELSE NULL 
    END cnt_entrega,

    VBAK.id_pedido id_pedido_vbak, --VBELN 'PVE-SAPS4-' prefijo
    LIPS1.id_documento_entrega, --VBELN 'DCE-SAPS4-' prefijo
    LIPS1.num_correlativo num_correlativo_lips, --POSNR
    LIPS1.num_correlativo_documento_modelo, --VGPOS
    LIPS1.cod_documento_modelo, --VGBEL
    VBAP2.id_pedido id_pedido_vbap, --VBELN 'PVE-SAPS4-' prefijo
    VBAP2.num_correlativo num_correlativo_vbap--POSNR

  FROM VBAK
  JOIN VBAP2 ON VBAP2.id_pedido   = VBAK.id_pedido
  JOIN MARM2 ON MARM2.id_material = VBAP2.id_material
    AND MARM2.cod_unidad_medida   = VBAP2.cod_unidad_medida_venta

  JOIN MARM2_COMPLEMENTO ON MARM2_COMPLEMENTO.id_material = VBAP2.id_material
  JOIN LIPS1 ON LIPS1.cod_documento_modelo      = REPLACE(VBAP2.id_pedido,'PVE-SAPS4-','')
    AND LIPS1.num_correlativo_documento_modelo  = VBAP2.num_correlativo
    AND LIPS1.cnt_entregada_unidad_venta        !=  0

  JOIN UNIDAD_MEDIDA_VENTA3 ON UNIDAD_MEDIDA_VENTA3.id_pedido_vbak  = VBAK.id_pedido 
    AND UNIDAD_MEDIDA_VENTA3.id_pedido_vbap        =  VBAP2.id_pedido
    AND UNIDAD_MEDIDA_VENTA3.num_correlativo_vbap  =  VBAP2.num_correlativo
;

CREATE OR REPLACE TEMP TABLE CANTIDAD_ENTREGA3 as
  SELECT 
    a.*
  FROM CANTIDAD_ENTREGA_COMPLETA3 a
  INNER JOIN 
  (SELECT id_documento_entrega,num_correlativo_lips,count(COALESCE(cnt_entrega, 0)) cantidad
  FROM CANTIDAD_ENTREGA_COMPLETA3
  GROUP BY 1,2) b
  ON a.id_documento_entrega = b.id_documento_entrega
  AND a.num_correlativo_lips = b.num_correlativo_lips
  WHERE b.cantidad = 1 OR (b.cantidad = 2 AND a.cnt_entrega IS NOT NULL)
;


CREATE OR REPLACE TEMP TABLE CANTIDAD_TOTAL_DESVIACION_TABLA3 AS
  SELECT
    DISTINCT
    CASE 
      WHEN TVPOD1.id_documento_tvpod_origen = ENTREGA3.entrega 
        AND TVPOD1.num_correlativo          = ENTREGA3.posicion_entrega 
        AND TVPOD1.cnt_desviacion           <> 0 
        AND TVPOD1.cod_unidad_medida_venta  = UNIDAD_MEDIDA_VENTA3.unidad_medida_venta 
      THEN TVPOD1.cnt_desviacion 
      WHEN TVPOD1.id_documento_tvpod_origen = ENTREGA3.entrega 
        AND TVPOD1.num_correlativo          = ENTREGA3.posicion_entrega 
        AND TVPOD1.cnt_desviacion           <> 0 
        AND TVPOD1.cod_unidad_medida_venta  !=  UNIDAD_MEDIDA_VENTA3.unidad_medida_venta
        AND MARM.id_material                =   TVPOD1.id_material 
        AND MARM.cod_unidad_medida          =   TVPOD1.cod_unidad_medida_venta 
        AND MARM_COMPLEMENTO.id_material    =   TVPOD1.id_material 
        AND MARM_COMPLEMENTO.cod_unidad_medida  = TVPOD1.cod_unidad_medida_base 
      THEN ((TVPOD1.cnt_desviacion * MARM.num_numerador_conversion) / MARM.num_denominador_conversion) * MARM_COMPLEMENTO.num_denominador_conversion / MARM_COMPLEMENTO.num_numerador_conversion 
      ELSE NULL 
    END cnt_desviacion_total,

    VBAK.id_pedido id_pedido_vbak, --VBELN 'PVE-SAPS4-' prefijo
    LIPS1.id_documento_entrega, --VBELN 'DCE-SAPS4-' prefijo
    LIPS1.num_correlativo num_correlativo_lips, --POSNR
    LIPS1.num_correlativo_documento_modelo, --VGPOS
    LIPS1.cod_documento_modelo, --VGBEL
    VBAP2.id_pedido id_pedido_vbap, --VBELN 'PVE-SAPS4-' prefijo
    VBAP2.num_correlativo num_correlativo_vbap--POSNR

  FROM  LIPS1
  LEFT JOIN  VBAP2 ON LIPS1.cod_documento_modelo  = REPLACE(VBAP2.id_pedido,'PVE-SAPS4-','')
    AND LIPS1.num_correlativo_documento_modelo    = VBAP2.num_correlativo
    AND LIPS1.cnt_entregada_unidad_venta          <> 0

  LEFT JOIN TVPOD1 ON TVPOD1.id_documento_tvpod_origen = CASE WHEN REPLACE(LIPS1.id_documento_entrega,'DCE-SAPS4-','') <> '' THEN REPLACE(LIPS1.id_documento_entrega,'DCE-SAPS4-','') ELSE NULL END --REVISAR
    AND TVPOD1.num_correlativo  = LIPS1.num_correlativo

  JOIN  VBAK ON VBAP2.id_pedido       = VBAK.id_pedido
  LEFT JOIN  MARM ON MARM.id_material = VBAP2.id_material

  LEFT JOIN  MARM_COMPLEMENTO ON MARM_COMPLEMENTO.id_material = VBAP2.id_material

  JOIN UNIDAD_MEDIDA_VENTA3 ON UNIDAD_MEDIDA_VENTA3.id_pedido_vbak  = VBAK.id_pedido
    AND UNIDAD_MEDIDA_VENTA3.id_pedido_vbap                         = VBAP2.id_pedido
    AND UNIDAD_MEDIDA_VENTA3.num_correlativo_vbap                   = VBAP2.num_correlativo

  JOIN  ENTREGA3 ON ENTREGA3.id_pedido_vbak       = VBAK.id_pedido
    AND ENTREGA3.id_documento_entrega             = LIPS1.id_documento_entrega
    AND ENTREGA3.num_correlativo_lips             = LIPS1.num_correlativo
    AND ENTREGA3.num_correlativo_documento_modelo = LIPS1.num_correlativo_documento_modelo
    AND ENTREGA3.cod_documento_modelo             = LIPS1.cod_documento_modelo
    AND ENTREGA3.id_pedido_vbap                   = VBAP2.id_pedido
    AND ENTREGA3.num_correlativo_vbap             = VBAP2.num_correlativo
;


CREATE OR REPLACE TEMP TABLE CANTIDAD_TOTAL_DESVIACION_3 AS
  SELECT 
    a.*
  FROM CANTIDAD_TOTAL_DESVIACION_TABLA3 a
  INNER JOIN 
  (SELECT id_documento_entrega,num_correlativo_lips,count(COALESCE(cnt_desviacion_total, 0)) cantidad
  FROM CANTIDAD_TOTAL_DESVIACION_TABLA3
  GROUP BY 1,2) b
  ON a.id_documento_entrega = b.id_documento_entrega
  AND a.num_correlativo_lips = b.num_correlativo_lips
  WHERE b.cantidad = 1 OR (b.cantidad = 2 AND a.cnt_desviacion_total IS NOT NULL)
;


CREATE OR REPLACE TEMP TABLE FACTURA3 AS
  WITH 
  FACTURA_DEPURADA3 AS
  (
    SELECT
      DISTINCT
      CASE 
        WHEN TVPOD1.id_documento_tvpod_origen = ENTREGA3.entrega 
          AND TVPOD1.num_correlativo = ENTREGA3.posicion_entrega 
        THEN TVPOD1.cod_motivo_desviacion 
      END motivo_desviacion,
      CASE 
        WHEN TVPOD1.id_documento_tvpod_origen = ENTREGA3.entrega 
          AND TVPOD1.num_correlativo = ENTREGA3.posicion_entrega 
        THEN TVPOD1.fec_pedido 
      END fec_desviacion,
      CASE 
        WHEN VBRP1.cod_documento_modelo = ENTREGA3.entrega 
          AND VBRP1.num_correlativo_documento_modelo = ENTREGA3.posicion_entrega 
          AND VBRP1.cnt_material <> 0 
        THEN REPLACE(VBRP1.id_documento,'DOC-SAPS4-','') 
      END factura,
      CASE 
        WHEN VBRP1.cod_documento_modelo=ENTREGA3.entrega 
          AND VBRP1.num_correlativo_documento_modelo = ENTREGA3.posicion_entrega 
          AND VBRP1.cnt_material <> 0 
        THEN VBRP1.num_correlativo 
      END posicion_factura,

      VBAK.id_pedido id_pedido_vbak, --VBELN 'PVE-SAPS4-' prefijo
      LIPS1.id_documento_entrega, --VBELN 'DCE-SAPS4-' prefijo
      LIPS1.num_correlativo num_correlativo_lips, --POSNR
      LIPS1.num_correlativo_documento_modelo, --VGPOS
      LIPS1.cod_documento_modelo, --VGBEL
      VBAP2.id_pedido id_pedido_vbap, --VBELN 'PVE-SAPS4-' prefijo
      VBAP2.num_correlativo num_correlativo_vbap--POSNR

    FROM LIPS1
    LEFT JOIN VBAP2 ON LIPS1.cod_documento_modelo = REPLACE(VBAP2.id_pedido,'PVE-SAPS4-','')
      AND LIPS1.num_correlativo_documento_modelo  = VBAP2.num_correlativo
      AND LIPS1.cnt_entregada_unidad_venta        <> 0

    LEFT JOIN VBRP1 ON VBRP1.cod_documento_modelo = REPLACE(LIPS1.id_documento_entrega,'DCE-SAPS4-','')
      AND VBRP1.num_correlativo_documento_modelo  = LIPS1.num_correlativo

    LEFT JOIN TVPOD1 ON TVPOD1.id_documento_tvpod_origen  = CASE WHEN REPLACE(LIPS1.id_documento_entrega,'DCE-SAPS4-','') <> '' THEN REPLACE(LIPS1.id_documento_entrega,'DCE-SAPS4-','') ELSE NULL END --REVISAR
      AND TVPOD1.num_correlativo  = LIPS1.num_correlativo

    JOIN VBAK ON VBAP2.id_pedido  = VBAK.id_pedido
    JOIN VBRK1 ON VBRK1.id_documento_origen = REPLACE(VBRP1.id_documento,'DOC-SAPS4-','')

    JOIN ENTREGA3 ON ENTREGA3.id_pedido_vbak  = VBAK.id_pedido
      AND ENTREGA3.id_documento_entrega       = LIPS1.id_documento_entrega
      AND ENTREGA3.num_correlativo_lips       = LIPS1.num_correlativo
      AND ENTREGA3.num_correlativo_documento_modelo = LIPS1.num_correlativo_documento_modelo
      AND ENTREGA3.cod_documento_modelo             = LIPS1.cod_documento_modelo
      AND ENTREGA3.id_pedido_vbap                   = VBAP2.id_pedido
      AND ENTREGA3.num_correlativo_vbap             = VBAP2.num_correlativo
  ),

  ORDENAR AS (
    SELECT 
      A.*,
      ROW_NUMBER() OVER(PARTITION BY id_documento_entrega, num_correlativo_lips ORDER BY id_documento_entrega, num_correlativo_lips) AS ORDEN
    FROM FACTURA_DEPURADA3 A
  )

  SELECT B.* EXCEPT(ORDEN)
  FROM ORDENAR B
  WHERE ORDEN = 1
;


CREATE OR REPLACE TEMP TABLE CANTIDAD_FACTURADA_COMPLETO3 AS
  SELECT
  DISTINCT
    CASE 
      WHEN VBRP1.cod_documento_modelo = ENTREGA3.entrega 
        AND VBRP1.num_correlativo_documento_modelo  = ENTREGA3.posicion_entrega 
        AND VBRP1.cnt_material                      <> 0 
        AND VBRP1.cod_unidad_medida_venta           = UNIDAD_MEDIDA_VENTA3.unidad_medida_venta 
      THEN VBRP1.cnt_material 
      WHEN VBRP1.cod_documento_modelo = ENTREGA3.entrega 
        AND VBRP1.num_correlativo_documento_modelo    = ENTREGA3.posicion_entrega 
        AND VBRP1.cnt_material                        <> 0 
        AND VBRP1.cod_unidad_medida_venta             != UNIDAD_MEDIDA_VENTA3.unidad_medida_venta
        AND MARM2.id_material                         = VBRP1.id_material 
        AND MARM2.cod_unidad_medida                   = VBRP1.cod_unidad_medida_venta 
        AND MARM2_COMPLEMENTO.id_material             = VBRP1.id_material 
        AND MARM2_COMPLEMENTO.cod_unidad_medida       = UNIDAD_MEDIDA_VENTA3.unidad_medida_venta
      THEN ((VBRP1.cnt_material * MARM2.num_numerador_conversion) / MARM2.num_denominador_conversion) * MARM2_COMPLEMENTO.num_denominador_conversion / MARM2_COMPLEMENTO.num_numerador_conversion  
      ELSE NULL 
    END cnt_facturada,

    VBAK.id_pedido id_pedido_vbak, --VBELN 'PVE-SAPS4-' prefijo
    LIPS1.id_documento_entrega, --VBELN 'DCE-SAPS4-' prefijo
    LIPS1.num_correlativo num_correlativo_lips, --POSNR
    LIPS1.num_correlativo_documento_modelo, --VGPOS
    LIPS1.cod_documento_modelo, --VGBEL
    VBAP2.id_pedido id_pedido_vbap, --VBELN 'PVE-SAPS4-' prefijo
    VBAP2.num_correlativo num_correlativo_vbap,--POSNR
    VBRP1.mnt_peso_factura, 
    VBRP1.valor_factura

  FROM LIPS1
  LEFT JOIN VBAP2 ON LIPS1.cod_documento_modelo = REPLACE(VBAP2.id_pedido,'PVE-SAPS4-','')
    AND LIPS1.num_correlativo_documento_modelo  = VBAP2.num_correlativo
    AND LIPS1.cnt_entregada_unidad_venta        <> 0

  LEFT JOIN VBRP1 ON VBRP1.cod_documento_modelo = REPLACE(LIPS1.id_documento_entrega,'DCE-SAPS4-','')
  AND VBRP1.num_correlativo_documento_modelo    = LIPS1.num_correlativo

  JOIN VBAK ON VBAP2.id_pedido  = VBAK.id_pedido

  JOIN VBRK1 ON VBRK1.id_documento_origen = REPLACE(VBRP1.id_documento,'DOC-SAPS4-','')

  JOIN MARM2_COMPLEMENTO ON MARM2_COMPLEMENTO.id_material = VBAP2.id_material
  JOIN MARM2 ON MARM2.id_material = VBAP2.id_material
    AND MARM2.cod_unidad_medida   = VBAP2.cod_unidad_medida_venta

  JOIN UNIDAD_MEDIDA_VENTA3 ON UNIDAD_MEDIDA_VENTA3.id_pedido_vbak  = VBAK.id_pedido 
    AND UNIDAD_MEDIDA_VENTA3.id_pedido_vbap                          = VBAP2.id_pedido
    AND UNIDAD_MEDIDA_VENTA3.num_correlativo_vbap                    = VBAP2.num_correlativo

  JOIN ENTREGA3 ON ENTREGA3.id_pedido_vbak    = VBAK.id_pedido
    AND ENTREGA3.id_documento_entrega         = LIPS1.id_documento_entrega
    AND ENTREGA3.num_correlativo_lips         = LIPS1.num_correlativo
    AND ENTREGA3.num_correlativo_documento_modelo = LIPS1.num_correlativo_documento_modelo
    AND ENTREGA3.cod_documento_modelo             = LIPS1.cod_documento_modelo
    AND ENTREGA3.id_pedido_vbap                   = VBAP2.id_pedido
    AND ENTREGA3.num_correlativo_vbap             = VBAP2.num_correlativo
;


CREATE OR REPLACE TEMP TABLE CANTIDAD_FACTURADA3 AS
  SELECT 
    a.*
  FROM CANTIDAD_FACTURADA_COMPLETO3 a
  INNER JOIN 
  (SELECT id_documento_entrega,num_correlativo_lips,count(COALESCE(cnt_facturada, 0)) cantidad
  FROM CANTIDAD_FACTURADA_COMPLETO3
  GROUP BY 1,2) b
  ON a.id_documento_entrega = b.id_documento_entrega
  AND a.num_correlativo_lips = b.num_correlativo_lips
  WHERE b.cantidad = 1 OR (b.cantidad = 2 AND a.cnt_facturada IS NOT NULL)
;


CREATE OR REPLACE TEMP TABLE DETALLE_FACTURA3 AS
  SELECT
    DISTINCT
    CASE 
    WHEN VBRK1.id_documento_origen=FACTURA3.factura THEN VBRK1.cod_clase_documento END clase_factura,
    CASE 
    WHEN VBRK1.id_documento_origen=FACTURA3.factura THEN VBRK1.fec_documento END fec_factura,

    CASE 
      WHEN FACTURA3.factura IS NOT NULL 
        AND VBPA.id_documento = FACTURA3.factura 
        AND VBPA.cod_funcion_socio  = 'ZT' 
      THEN REPLACE(VBPA.id_cliente,'ITL-SAPS4-','')
      WHEN FACTURA3.factura IS NULL 
        AND VBPA_COMPLEMENTO.id_documento = REPLACE(VBAK.id_pedido,'PVE-SAPS4-','') 
        AND VBPA_COMPLEMENTO.cod_funcion_socio  = 'ZT' 
      THEN REPLACE(VBPA_COMPLEMENTO.id_cliente,'ITL-SAPS4-','') 
    END territorio,

    CASE 
      WHEN VBAP4.cod_documento_modelo = REPLACE(VBRP1.id_documento,'DOC-SAPS4-','') 
        AND VBAP4.num_correlativo_documento_modelo  = VBRP1.num_correlativo 
      THEN REPLACE(VBAP4.id_pedido,'PVE-SAPS4-','') 
    END documento_rechazo,

    CASE
      WHEN REPLACE(VBAP4.id_pedido,'PVE-SAPS4-','') = (CASE WHEN VBAP4.cod_documento_modelo = REPLACE(VBRP1.id_documento,'DOC-SAPS4-','') 
                                                        AND VBAP4.num_correlativo_documento_modelo=VBRP1.num_correlativo 
                                                        THEN REPLACE(VBAP4.id_pedido,'PVE-SAPS4-','') END) 
      AND VBAP4.cnt_acumulada_venta <> 0 
      THEN VBAP4.num_correlativo 
    END posicion_rechazo,

    VBAK.id_pedido id_pedido_vbak, --VBELN 'PVE-SAPS4-' prefijo
    LIPS1.id_documento_entrega, --VBELN 'DCE-SAPS4-' prefijo
    LIPS1.num_correlativo num_correlativo_lips, --POSNR
    LIPS1.num_correlativo_documento_modelo, --VGPOS
    LIPS1.cod_documento_modelo, --VGBEL
    VBAP4.id_pedido id_pedido_vbap, --VBELN 'PVE-SAPS4-' prefijo
    VBAP4.num_correlativo num_correlativo_vbap,--POSNR
    FACTURA3.factura,
    FACTURA3.posicion_factura

  FROM LIPS1
  LEFT JOIN VBRP1 ON VBRP1.cod_documento_modelo = REPLACE(LIPS1.id_documento_entrega,'DCE-SAPS4-','')
    AND VBRP1.num_correlativo_documento_modelo  = LIPS1.num_correlativo

  LEFT JOIN VBAP4 ON VBAP4.cod_documento_modelo = REPLACE(VBRP1.id_documento,'DOC-SAPS4-','')
    AND VBAP4.num_correlativo_documento_modelo  = VBRP1.num_correlativo

  JOIN VBRK1 ON VBRK1.id_documento_origen = REPLACE(VBRP1.id_documento,'DOC-SAPS4-','')

  LEFT JOIN VBAK ON REPLACE(VBAK.id_pedido,'PVE-SAPS4-','') = LIPS1.cod_documento_modelo

  LEFT JOIN VBPA ON VBPA.id_documento = VBRK1.id_documento_origen
    AND VBPA.cod_funcion_socio        = 'ZT'
    AND VBPA.num_correlativo          = 0

  LEFT JOIN VBPA VBPA_COMPLEMENTO ON VBPA_COMPLEMENTO.id_documento  = REPLACE(VBAK.id_pedido,'PVE-SAPS4-','')
    AND VBPA_COMPLEMENTO.cod_funcion_socio                          = 'ZT'
    AND VBPA_COMPLEMENTO.num_correlativo                            = 0

  JOIN FACTURA3 ON FACTURA3.id_pedido_vbak  = VBAK.id_pedido
    AND FACTURA3.id_documento_entrega       = LIPS1.id_documento_entrega
    AND FACTURA3.num_correlativo_lips       = LIPS1.num_correlativo
    AND FACTURA3.num_correlativo_documento_modelo = LIPS1.num_correlativo_documento_modelo
    AND FACTURA3.cod_documento_modelo             = LIPS1.cod_documento_modelo
;


CREATE OR REPLACE TEMP TABLE RECHAZO_COMPLETO3 AS
  SELECT
    DISTINCT
    CASE 
    WHEN VBAK4.id_pedido_origen = DETALLE_FACTURA3.documento_rechazo THEN VBAK4.fec_creacion END fec_rechazo_factura,

    CASE 
      WHEN REPLACE(VBAP4.id_pedido,'PVE-SAPS4-','') = DETALLE_FACTURA3.documento_rechazo 
      AND VBAP4.cnt_acumulada_venta <> 0 
      THEN VBAP4.num_correlativo 
    END posicion_rechazo,

    CASE
    WHEN VBAK4.id_pedido=VBAP4.id_pedido THEN VBAK4.cod_motivo_pedido end cod_motivo_rechazo_del_rechazo,

    VBAK4.id_pedido id_pedido_vbak, --VBELN 'PVE-SAPS4-' prefijo
    LIPS1.id_documento_entrega, --VBELN 'DCE-SAPS4-' prefijo
    LIPS1.num_correlativo num_correlativo_lips, --POSNR
    LIPS1.num_correlativo_documento_modelo, --VGPOS
    LIPS1.cod_documento_modelo, --VGBEL
    VBAP4.id_pedido id_pedido_vbap, --VBELN 'PVE-SAPS4-' prefijo
    VBAP4.num_correlativo num_correlativo_vbap,--POSNR
    DETALLE_FACTURA3.factura,
    DETALLE_FACTURA3.posicion_factura,
    DETALLE_FACTURA3.documento_rechazo

  FROM LIPS1
  LEFT JOIN VBRP1 ON VBRP1.cod_documento_modelo   = REPLACE(LIPS1.id_documento_entrega,'DCE-SAPS4-','')
    AND VBRP1.num_correlativo_documento_modelo    = LIPS1.num_correlativo

  LEFT JOIN VBAP4 ON VBAP4.cod_documento_modelo = REPLACE(VBRP1.id_documento,'DOC-SAPS4-','')
    AND VBAP4.num_correlativo_documento_modelo  = VBRP1.num_correlativo

  LEFT JOIN VBAK4 ON VBAK4.id_pedido  = VBAP4.id_pedido
  LEFT JOIN VBRK1 ON VBRK1.id_documento_origen  = REPLACE(VBRP1.id_documento,'DOC-SAPS4-','')

  JOIN DETALLE_FACTURA3 --ON DETALLE_FACTURA3.id_pedido_vbak=VBAK4.id_pedido
  ON DETALLE_FACTURA3.id_documento_entrega                  = LIPS1.id_documento_entrega
    AND DETALLE_FACTURA3.num_correlativo_lips               = LIPS1.num_correlativo
    AND DETALLE_FACTURA3.num_correlativo_documento_modelo   = LIPS1.num_correlativo_documento_modelo
    AND DETALLE_FACTURA3.cod_documento_modelo               = LIPS1.cod_documento_modelo
;


CREATE OR REPLACE TEMP TABLE RECHAZO3 AS 
  WITH count_rechazo AS 
  (
    SELECT 
      id_documento_entrega,
      num_correlativo_lips,
      count(COALESCE(posicion_rechazo, 0)) posicion_rechazo
    FROM RECHAZO_COMPLETO3
    GROUP BY 1,2
  ),
  inner_rechazo AS 
  (
    SELECT 
      DISTINCT a.*
    FROM RECHAZO_COMPLETO3 a
    INNER JOIN
    count_rechazo b
    ON a.id_documento_entrega = b.id_documento_entrega
    AND a.num_correlativo_lips = b.num_correlativo_lips
    WHERE b.posicion_rechazo = 1 OR (b.posicion_rechazo >1 AND a.posicion_rechazo IS NOT NULL)
  )

  SELECT * FROM inner_rechazo
;


CREATE OR REPLACE TEMP TABLE CANTIDAD_RECHAZO_COMPLETO3 AS
  SELECT
    DISTINCT
    CASE
      WHEN REPLACE(VBAP4.id_pedido,'PVE-SAPS4-','')   =   DETALLE_FACTURA3.documento_rechazo 
        AND VBAP4.cnt_acumulada_venta                 <>  0 
        AND UNIDAD_MEDIDA_VENTA3.unidad_medida_venta  =   VBAP4.cod_unidad_medida_venta 
      THEN VBAP4.cnt_acumulada_venta 
      WHEN REPLACE(VBAP4.id_pedido,'PVE-SAPS4-','')   =   DETALLE_FACTURA3.documento_rechazo 
        AND VBAP4.cnt_acumulada_venta                 <>  0 
        AND UNIDAD_MEDIDA_VENTA3.unidad_medida_venta  !=  VBAP4.cod_unidad_medida_venta
        AND MARM2.id_material                          =  VBAP4.id_material 
        AND MARM2.cod_unidad_medida                    =  VBAP4.cod_unidad_medida_venta 
        AND MARM2_COMPLEMENTO.id_material              =  VBAP4.id_material 
        AND MARM2_COMPLEMENTO.cod_unidad_medida        =  UNIDAD_MEDIDA_VENTA3.unidad_medida_venta
      THEN ((VBAP4.cnt_acumulada_venta * MARM2.num_numerador_conversion) / MARM2.num_denominador_conversion) * MARM2_COMPLEMENTO.num_denominador_conversion / MARM2_COMPLEMENTO.num_numerador_conversion 
      ELSE NULL 
    END cnt_rechazo,

    VBAK4.id_pedido id_pedido_vbak, --VBELN 'PVE-SAPS4-' prefijo
    LIPS1.id_documento_entrega, --VBELN 'DCE-SAPS4-' prefijo
    LIPS1.num_correlativo num_correlativo_lips, --POSNR
    LIPS1.num_correlativo_documento_modelo, --VGPOS
    LIPS1.cod_documento_modelo, --VGBEL
    VBAP4.id_pedido id_pedido_vbap, --VBELN 'PVE-SAPS4-' prefijo
    VBAP4.num_correlativo num_correlativo_vbap,--POSNR
    DETALLE_FACTURA3.factura,
    DETALLE_FACTURA3.posicion_factura,
    DETALLE_FACTURA3.posicion_rechazo,
    DETALLE_FACTURA3.documento_rechazo,
    VBAP4.mnt_peso_rechazo,
    VBAP4.valor_rechazo

  FROM LIPS1
  LEFT JOIN VBRP1 ON VBRP1.cod_documento_modelo = REPLACE(LIPS1.id_documento_entrega,'DCE-SAPS4-','')
    AND VBRP1.num_correlativo_documento_modelo  = LIPS1.num_correlativo

  LEFT JOIN VBAP4 ON VBAP4.cod_documento_modelo = REPLACE(VBRP1.id_documento,'DOC-SAPS4-','')
    AND VBAP4.num_correlativo_documento_modelo  = VBRP1.num_correlativo

  LEFT JOIN VBAK4 ON VBAK4.id_pedido  = VBAP4.id_pedido
  LEFT JOIN VBRK1 ON VBRK1.id_documento_origen  = REPLACE(VBRP1.id_documento,'DOC-SAPS4-','')

  LEFT JOIN MARM2 ON MARM2.id_material  = VBAP4.id_material
    AND MARM2.cod_unidad_medida         = VBAP4.cod_unidad_medida_venta

  LEFT JOIN MARM2_COMPLEMENTO ON MARM2_COMPLEMENTO.id_material=VBAP4.id_material

  JOIN DETALLE_FACTURA3 --ON DETALLE_FACTURA3.id_pedido_vbak=VBAK4.id_pedido
  ON DETALLE_FACTURA3.id_documento_entrega                = LIPS1.id_documento_entrega
    AND DETALLE_FACTURA3.num_correlativo_lips             = LIPS1.num_correlativo
    AND DETALLE_FACTURA3.num_correlativo_documento_modelo = LIPS1.num_correlativo_documento_modelo
    AND DETALLE_FACTURA3.cod_documento_modelo             = LIPS1.cod_documento_modelo

  LEFT JOIN UNIDAD_MEDIDA_VENTA3 --ON UNIDAD_MEDIDA_VENTA3.id_pedido_vbak=VBAK.id_pedido 
  ON UNIDAD_MEDIDA_VENTA3.id_material = VBAP4.id_material
;


CREATE OR REPLACE TEMP TABLE CANTIDAD_RECHAZO3 AS 
  WITH count_cnt_rechazo AS 
  (
    SELECT 
      id_documento_entrega,
      num_correlativo_lips,
      count(COALESCE(cnt_rechazo, 0)) cantidad
    FROM CANTIDAD_RECHAZO_COMPLETO3
    GROUP BY 1,2
  ),
  inner_cnt_rechazo AS 
  (
    SELECT 
      a.*
    FROM CANTIDAD_RECHAZO_COMPLETO3 a
    INNER JOIN 
    count_cnt_rechazo b
    ON a.id_documento_entrega = b.id_documento_entrega
    AND a.num_correlativo_lips = b.num_correlativo_lips
    WHERE b.cantidad = 1 OR (b.cantidad > 1 AND a.cnt_rechazo IS NOT NULL)
  )
  SELECT * FROM inner_cnt_rechazo
;


CREATE OR REPLACE TEMP TABLE TABLA_PREVIA_FINAL_3 AS 
  SELECT 
      INDEPENDIENTE3.*,
      CANTIDAD_FACTURADA3.mnt_peso_factura,
      CANTIDAD_FACTURADA3.valor_factura,
      ENTREGA3.entrega,
      ENTREGA3.posicion_entrega,
      CANTIDAD_ENTREGA3.cnt_entrega,
      UNIDAD_MEDIDA_VENTA3.unidad_medida_venta,
      CANTIDAD3.cnt,
      ENTREGA3.motivo_desviacion,
      ENTREGA3.fec_desviacion,
      FACTURA3.factura,
      FACTURA3.posicion_factura,
      DETALLE_FACTURA3.clase_factura,
      DETALLE_FACTURA3.fec_factura,
      --DETALLE_FACTURA3.territorio,
      CANTIDAD_FACTURADA3.cnt_facturada,
      DETALLE_FACTURA3.documento_rechazo,
      RECHAZO3.fec_rechazo_factura,
      RECHAZO3.posicion_rechazo,
      RECHAZO3.cod_motivo_rechazo_del_rechazo,
      CANTIDAD_RECHAZO3.cnt_rechazo,
      CANTIDAD_RECHAZO3.mnt_peso_rechazo,
      CANTIDAD_RECHAZO3.valor_rechazo,
      CANTIDAD_TOTAL_DESVIACION_3.cnt_desviacion_total,
      3 AS flg
  FROM INDEPENDIENTE3
  JOIN ENTREGA3
  ON INDEPENDIENTE3.id_documento_entrega    = ENTREGA3.id_documento_entrega
    AND INDEPENDIENTE3.num_correlativo_lips = ENTREGA3.num_correlativo_lips

  LEFT JOIN UNIDAD_MEDIDA_VENTA3
  ON UNIDAD_MEDIDA_VENTA3.id_pedido_vbap          = ENTREGA3.id_pedido_vbap
    AND UNIDAD_MEDIDA_VENTA3.num_correlativo_vbap = ENTREGA3.num_correlativo_vbap

  LEFT JOIN CANTIDAD3
  ON CANTIDAD3.id_pedido_vbap     = ENTREGA3.id_pedido_vbap
    AND CANTIDAD3.num_correlativo = ENTREGA3.num_correlativo_vbap

  JOIN CANTIDAD_ENTREGA3
  ON INDEPENDIENTE3.id_documento_entrega    = CANTIDAD_ENTREGA3.id_documento_entrega
    AND INDEPENDIENTE3.num_correlativo_lips = CANTIDAD_ENTREGA3.num_correlativo_lips

  LEFT JOIN CANTIDAD_TOTAL_DESVIACION_3
  ON CANTIDAD_TOTAL_DESVIACION_3.id_documento_entrega     = ENTREGA3.id_documento_entrega
    AND CANTIDAD_TOTAL_DESVIACION_3.num_correlativo_lips  = ENTREGA3.num_correlativo_lips

  LEFT JOIN FACTURA3
  ON FACTURA3.id_documento_entrega    = ENTREGA3.id_documento_entrega
    AND FACTURA3.num_correlativo_lips = ENTREGA3.num_correlativo_lips

  LEFT JOIN CANTIDAD_FACTURADA3
  ON CANTIDAD_FACTURADA3.id_documento_entrega     = ENTREGA3.id_documento_entrega
    AND CANTIDAD_FACTURADA3.num_correlativo_lips  = ENTREGA3.num_correlativo_lips

  LEFT JOIN DETALLE_FACTURA3
  ON DETALLE_FACTURA3.id_documento_entrega      = ENTREGA3.id_documento_entrega
    AND DETALLE_FACTURA3.num_correlativo_lips   = ENTREGA3.num_correlativo_lips


  LEFT JOIN RECHAZO3
  ON RECHAZO3.id_documento_entrega    = ENTREGA3.id_documento_entrega
    AND RECHAZO3.num_correlativo_lips = ENTREGA3.num_correlativo_lips
    AND RECHAZO3.posicion_rechazo     = DETALLE_FACTURA3.posicion_rechazo
    AND RECHAZO3.documento_rechazo    = DETALLE_FACTURA3.documento_rechazo

  LEFT JOIN CANTIDAD_RECHAZO3
  ON CANTIDAD_RECHAZO3.id_documento_entrega     = ENTREGA3.id_documento_entrega
    AND CANTIDAD_RECHAZO3.num_correlativo_lips  = ENTREGA3.num_correlativo_lips
    AND CANTIDAD_RECHAZO3.posicion_rechazo      = DETALLE_FACTURA3.posicion_rechazo
    AND CANTIDAD_RECHAZO3.documento_rechazo     = DETALLE_FACTURA3.documento_rechazo
;


/****************************************************************************************/
/************************ TABLA UNION GENERAL *******************************************/
/****************************************************************************************/
CREATE OR REPLACE TEMP TABLE  TABLA_UNION_GENERAL AS 
  SELECT * FROM TABLA_PREVIA_FINAL_1
  UNION ALL
  SELECT * FROM TABLA_PREVIA_FINAL_2
  UNION ALL
  SELECT * FROM TABLA_PREVIA_FINAL_3
;


/***********************FECHA RECHAZO CON TABLAS BRONZE**********************/
CREATE OR REPLACE TEMP TABLE  TABLA_FECHA_RECHAZO AS
  SELECT
      VBAP.id_pedido,
      VBAP.num_correlativo,
      MAX(a.fec_creacion) as fec_rechazo 
  FROM `{silver_project_id}.slv_modelo_ventas.documento_modificacion_detalle` a
  INNER JOIN `{silver_project_id}.slv_modelo_ventas.s4_pedido_detalle` VBAP
  ON a.periodo                                      >= var_fecha_inicio --Filtro particion tabla "documento_modificacion_detalle"
    AND a.periodo                                   <=  var_fecha_actual
    AND a.fec_creacion                              >= var_fecha_inicio
    AND a.fec_creacion                              <=  var_fecha_actual
    AND VBAP.periodo                                >= var_fecha_inicio
    AND VBAP.periodo                                <=  var_fecha_actual
    AND LTRIM(a.cod_objeto,'0')                     =   LTRIM(REPLACE(VBAP.id_pedido,'PVE-SAPS4-',''),'0')
    AND RIGHT(a.cod_clave_tabla_modificada,6)       =   LPAD(SAFE_CAST(num_correlativo AS STRING), 6, '0')
    AND a.cod_clase_objeto                          =   'VERKBELEG'
    AND a.nom_campo                                 =   'ABGRU'
    AND a.val_nuevo_campo is not null  
  GROUP BY
  VBAP.id_pedido,
  VBAP.num_correlativo
;


/***********************CRUCE CON TABLA UNION GENERAL**********************/
CREATE OR REPLACE TEMP TABLE TABLA_UNION_GENERAL_2 AS 
  SELECT 
    a.*,

    CASE
      WHEN b.fec_rechazo IS NOT NULL 
        AND a.motivo_rechazo is not null 
        AND flg in (1,2) 
      THEN b.fec_rechazo
      WHEN b.fec_rechazo IS NULL 
        AND a.motivo_rechazo is not null 
        AND flg in (1,2) 
      THEN a.fec_creacion
    END fec_rechazo,

    (IFNULL(a.cnt_entrega, 0) - IFNULL(a.cnt_facturada, 0)) as cnt_total_desviacion

  FROM TABLA_UNION_GENERAL a
  LEFT JOIN TABLA_FECHA_RECHAZO b
  on a.documento_venta  = REPLACE(b.id_pedido,'PVE-SAPS4-','')
  and a.posicion        = b.num_correlativo
  and flg in (1,2)
;


-------- INICIO BUCLE
CREATE OR REPLACE TEMP TABLE  MARA as
  SELECT
    a.id_material_origen,
    a.id_material_reemplazo,
    a.fec_creacion,
    a.cod_unidad_medida_base
  FROM  `{silver_project_id}.slv_modelo_material.material` a
  WHERE a.des_origen = 'SAPS4' 
  AND a.cod_tipo_material in ("ZFER","ZHAW")
;
------- FIN BUCLE


CREATE OR REPLACE TEMP TABLE TABLA_UNION_GENERAL_3_1 AS 
  SELECT
    b.*,
    a.id_material_reemplazo cod_material_actual
  FROM MARA a
  INNER JOIN TABLA_UNION_GENERAL_2 b
  ON LTRIM(a.id_material_origen,'0') = LTRIM(b.material_historico, '0')
;


/* ---CASO REGULAR QUE ESTA ASOCIANDO PEDIDO - NUM_CORRELATIVO (vbpa,vbak,vbrk,likp)*/ 
CREATE OR REPLACE TEMP TABLE INTERLOCUTOR_PEDIDO AS
  SELECT  
    periodo,
    des_origen,
    id_documento_interlocutor,
    id_documento_detalle,
    REPLACE(id_documento, 'PVE-SAPS4-', '') id_documento,
    num_correlativo,
    tip_documento,
    fec_documento,
    cod_funcion_socio,
    id_cliente,
    id_proveedor,
    cod_contacto,
    cod_pais,
    flg_determinacion_precio,
    flg_cliente_rappel,
    cod_zona_transporte,
    cod_asignacion_jerarquia,
    fec_proceso 
  FROM `{silver_project_id}.slv_modelo_ventas.s4_documento_interlocutor`  a
  WHERE a.periodo >= var_fecha_inicio  
    AND a.periodo <= var_fecha_actual
    AND cod_funcion_socio = 'ZT'
;


CREATE OR REPLACE TEMP TABLE TABLA_UNION_GENERAL_3 AS 
  SELECT
    a.*,
    CASE WHEN b.id_cliente is not null 
      THEN REPLACE(b.id_cliente,'ITL-SAPS4-','') 
      ELSE REPLACE(c.id_cliente,'ITL-SAPS4-','') 
    END AS territorio_1

  FROM TABLA_UNION_GENERAL_3_1 a
  LEFT JOIN INTERLOCUTOR_PEDIDO b
  ON  LTRIM(a.documento_venta,'0')  = LTRIM(b.id_documento, '0')
    AND a.posicion                  = b.num_correlativo
  LEFT JOIN INTERLOCUTOR_PEDIDO C
  ON LTRIM(a.documento_venta,'0') = LTRIM(c.id_documento, '0')
;


/************RENOMBRAR TABLAS POR NOMBRES DE ACUERDO A NOMENCLATURA DATALAKE*************/
CREATE OR REPLACE TEMP TABLE gold_fill_rate_nombres AS 
  SELECT 
    DISTINCT
    mnt_peso_neto,
    cod_unidad_medida_peso,
    mnt_neto_pedido,
    cod_moneda,
    (mnt_peso_neto/NULLIF(cnt, 0))   factor_peso,  
    (mnt_neto_pedido/NULLIF(cnt, 0)) factor_valor,
    ((mnt_neto_pedido/NULLIF(cnt, 0))*NULLIF(cnt_entrega, 0)) valor_neto_entrega,
    sociedad as cod_sociedad_factura,
    organizacion_venta as cod_organizacion_venta,
    canal as cod_canal_distribucion,
    sector as cod_sector_comercial,
    cliente as cod_cliente,
    cod_jerarquia_nivel_1,
    cod_jerarquia_nivel_2,
    destinatario_mercancia as cod_interlocutor_destino,
    documento_venta as cod_pedido_venta,
    clase_documento_venta as cod_clase_pedido,
    condicion_pago as cod_condicion_pago,
    bloqueo_entrega as cod_bloqueo_entrega,
    condicion_expedicion as cod_condicion_expedicion,
    posicion as num_correlativo_pedido,
    material_historico as cod_material_historico,
    cod_material_actual,
    unidad_medida_venta as cod_unidad_comercial,
    cnt as cnt_pedido,
    fec_creacion,
    fec_rechazo_factura,
    motivo_rechazo as cod_motivo_rechazo,
    fec_rechazo,
    centro as cod_centro,
    puesto_expedicion as cod_punto_expedicion,
    pais as cod_pais,
    region as cod_region,
    poblacion des_poblacion,
    distrito as des_distrito,
    zona_venta as cod_zona_cliente,
    oficina_venta as cod_oficina_venta,
    grupo_cliente as cod_grupo_cliente,
    grupo_cliente_2 as cod_segmento_cliente,
    grupo_condicion as cod_grupo_condicion,
    grupo_precio as cod_grupo_precio,
    grupo_vendedor as cod_grupo_vendedor,
    lista_precio as cod_categoria_precio,
    marca as cod_marca,
    COD_DUENIO_MARCA,
    DES_DUENIO_MARCA,
    plataforma as cod_plataforma,
    subplataforma as cod_sub_plataforma,
    categoia as cod_categoria,
    familia as cod_familia,
    variedad as cod_variedad,
    presentacion as cod_presentacion,
    negocio as cod_negocio,
    subnegocio as cod_subnegocio,
    tip_material as cod_tipo_material,
    cnt_entrega,
    cnt_total_desviacion,
    motivo_desviacion as cod_motivo_desviacion,
    fec_desviacion as fec_pedido,
    cnt_facturada,
    --territorio as cod_interlocutor_territorio,
    territorio_1 as cod_interlocutor_territorio,
    cod_motivo_rechazo_del_rechazo,
    cnt_rechazo,
    fec_factura,
    flg,
    id_documento_entrega,
    num_correlativo_lips,
    num_correlativo_documento_modelo,
    cod_documento_modelo,
    entrega,
    posicion_entrega,
    factura,
    posicion_factura,
    clase_factura,
    documento_rechazo,
    posicion_rechazo,
    mnt_peso_entrega,
    mnt_peso_factura,
    valor_factura,
    cnt_desviacion_total,
    mnt_peso_rechazo,
    valor_rechazo,
    ((mnt_peso_neto/NULLIF(cnt, 0)) * cnt_desviacion_total)    mnt_peso_desviacion,
    ((mnt_neto_pedido/NULLIF(cnt, 0)) * cnt_desviacion_total)  mnt_neto_desviacion
  FROM TABLA_UNION_GENERAL_3
;
/************FIN RENOMBRAR RENOMBRAR TABLAS DE ACUERDO A NOMENCLATURA DATALAKE*************/



/************AGREGAR DESCRIPCIONES*************/
CREATE OR REPLACE TEMP TABLE gold_fill_rate as
  SELECT * FROM (
  SELECT 
    DISTINCT
    id_documento_entrega,
    num_correlativo_lips,
    num_correlativo_documento_modelo,
    cod_documento_modelo,
    entrega,
    posicion_entrega,
    factura,
    posicion_factura,
    clase_factura,
    documento_rechazo,
    posicion_rechazo,
    cod_sociedad_factura,
    t0.des_descripcion      AS des_sociendad_factura,
    al.cod_organizacion_venta,
    tk.des_descripcion      AS des_organizacion_venta,
    cod_canal_distribucion,
    tw.des_descripcion      AS des_canal_distribucion,
    al.cod_sector_comercial,
    ts.des_descripcion      AS des_sector_comercial,
    cod_cliente,
    tor.nom_interlocutor,
    cod_jerarquia_nivel_1,
    jer_1.nom_interlocutor as des_jerarquia_nivel_1,
    cod_jerarquia_nivel_2,
    jer_2.nom_interlocutor as des_jerarquia_nivel_2,
    cod_interlocutor_destino,
    ti.nom_interlocutor     AS nom_interlocutor_destino,
    cod_pedido_venta,
    cod_clase_pedido,
    ak.des_descripcion      AS des_clase_pedido,
    cod_condicion_pago,
    vz.des_descripcion      AS des_condicion_pago,
    cod_bloqueo_entrega,
    vls.des_descripcion     AS des_bloqueo_entrega,
    cod_condicion_expedicion,
    vs.des_descripcion      AS des_condicion_expedicion,
    num_correlativo_pedido,
    cod_material_historico,
    ma.des_material, /*A*/
    cod_material_actual,
    mkt.des_material        AS des_material_actual,
    cod_unidad_comercial,
    al.cnt_pedido,
    al.fec_creacion,
    fec_rechazo_factura,
    al.cod_motivo_rechazo,
    ag.des_descripcion      AS des_motivo_rechazo,
    -- cnt_rechazada_pedido,
    al.fec_rechazo,
    al.cod_centro,
    os.des_centro, /*A*/
    al.cod_punto_expedicion,
    ste.des_descripcion     AS des_punto_expedicion,
    al.cod_pais,
    tt.des_descripcion      AS des_pais,
    al.cod_region,
    re.des_descripcion      AS des_region,
    al.des_poblacion,
    al.des_distrito,
    cod_zona_cliente,
    t7.des_descripcion      AS des_zona_cliente,
    cod_oficina_venta,
    tb.des_descripcion      AS des_oficina_venta,
    cod_grupo_cliente,
    t5.des_descripcion      AS des_grupo_cliente,
    cod_segmento_cliente,
    al.cod_grupo_condicion,
    gg.des_descripcion      AS des_grupo_condicion,
    cod_grupo_precio,
    t8.des_descripcion      AS des_grupo_precio,
    cod_grupo_vendedor,
    ctv.des_descripcion     AS des_grupo_vendedor,
    cod_categoria_precio,
    t00.des_descripcion     AS des_categoria_precio,
    al.cod_marca,
    tma.des_descripcion     AS des_marca,
    al.COD_DUENIO_MARCA,
    al.DES_DUENIO_MARCA,
    al.cod_plataforma,
    a.des_plataforma,
    al.cod_sub_plataforma,
    a.des_sub_plataforma,
    al.cod_categoria,
    a.des_categoria,
    al.cod_familia,
    a.des_familia,
    al.cod_variedad,
    a.des_variedad,
    al.cod_presentacion,/*observado*/
    al.cod_negocio,
    tm.des_descripcion      AS des_negocio,
    al.cod_subnegocio,
    al.cod_tipo_material,
    ma.des_tipo_material, /*A*/
    cnt_entrega             AS cnt_entregada_unidad_venta,
    cnt_total_desviacion,
    cnt_desviacion_total,
    cod_motivo_desviacion,
    pod.des_descripcion     AS des_motivo_desviacion,
    al.fec_pedido,
    cnt_facturada           AS cnt_material,
    cod_interlocutor_territorio,
    lo.nom_interlocutor     AS nom_interlocutor_territorio,
    al.cod_motivo_rechazo_del_rechazo,
    vau.des_descripcion     AS des_motivo_rechazo_del_rechazo,
    cnt_rechazo             AS cnt_rechazada,

    CASE
      WHEN al.cod_motivo_rechazo IN ('28','33','27','78','79','38','35','39','30','43','36','45','37','82') THEN 'Pedidos no procesados'
      WHEN al.cod_motivo_rechazo IN ('53','77') THEN 'Créditos y cobranzas'
      WHEN al.cod_motivo_rechazo IN ('80','81','48','83') THEN 'Gestión comercial'
      WHEN al.cod_motivo_rechazo IN ('51') THEN 'Disponibilidad de inventario'
      WHEN al.cod_motivo_rechazo IN ('52','34') THEN 'Restricción logística'
    END nivel_1_motivo_rechazo,

    CASE
      WHEN al.cod_motivo_rechazo IN ('28','33','27','78','79','80','81','48','83') THEN 'Comercial'
      WHEN al.cod_motivo_rechazo IN ('38','35','39','30','43','36','45','37','51','82') THEN 'Planeamiento'
      WHEN al.cod_motivo_rechazo IN ('53','77') THEN 'Créditos'
      WHEN al.cod_motivo_rechazo IN ('52','34') THEN 'Distribución'
    END area_responsable_motivo_rechazo,

    CASE
      WHEN al.cod_motivo_rechazo IN ('28') THEN 'INH - Nuevo producto'
      WHEN al.cod_motivo_rechazo IN ('33') THEN 'INH - Producto discontinuado'
      WHEN al.cod_motivo_rechazo IN ('27') THEN 'INH - Decisión comercial'
      WHEN al.cod_motivo_rechazo IN ('78','79') THEN 'Diferencia de precio cadena'
      WHEN al.cod_motivo_rechazo IN ('38','35','39','30','43','36','45','37','82') THEN 'INH - Falta de stock'
      WHEN al.cod_motivo_rechazo IN ('53') THEN 'Rechazo por crédito'
      WHEN al.cod_motivo_rechazo IN ('77') THEN 'Rechazo por aprobador'
      WHEN al.cod_motivo_rechazo IN ('80') THEN 'Error en el pedido'
      WHEN al.cod_motivo_rechazo IN ('81','83') THEN 'No atendido por bloqueo entrega'
      WHEN al.cod_motivo_rechazo IN ('48') THEN 'Cancelación de orden cliente'
      WHEN al.cod_motivo_rechazo IN ('51') THEN 'No atendido falta stock planeamiento'
      WHEN al.cod_motivo_rechazo IN ('52','34') THEN 'No atendido por distribución'
    END nivel_2_simplificado_rechazo,

    CASE
      WHEN al.cod_motivo_rechazo_del_rechazo IN ('D06','D04','D01','D07','D05','D08','D03','C01','C02','D02','V01','V05','V08','V03','V09','999','V04','V07','V02','V06') THEN 'Rechazos'
      ELSE null
    END nivel_1_motivo_rechazo_del_rechazo,

    CASE
      WHEN al.cod_motivo_rechazo_del_rechazo IN ('D06','D04','D01','D07','D05','D08','D03') THEN 'Rechazos por distribución'
      WHEN al.cod_motivo_rechazo_del_rechazo IN ('C01','C02','D02') THEN 'Rechazos por calidad del producto'
      WHEN al.cod_motivo_rechazo_del_rechazo IN ('V01','V05','V08','V03','V09') THEN 'Rechazo comercial por inconsistencia de data'
      WHEN al.cod_motivo_rechazo_del_rechazo IN ('999','V04','V07','V02','V06') THEN 'Rechazo responsabilidad Cliente'
    END AS nivel_2_simplificado_rechazo_del_rechazo,

    CASE
      WHEN al.cod_motivo_rechazo_del_rechazo IN ('D06','D01','D07','D04', 'D05','D08','D03') THEN 'Distribución'
      WHEN al.cod_motivo_rechazo_del_rechazo IN ('C01','C02','D02') THEN 'Planeamiento'
      WHEN al.cod_motivo_rechazo_del_rechazo IN ('V01','V05','V08','V03','V09','999','V04','V07','V02','V06') THEN 'Comercial'
    END area_responsable_motivo_rechazo_del_rechazo,


    CASE
      WHEN al.cod_motivo_desviacion IN ('ZD06','ZD04','ZD01','ZD07','ZD05','ZD08','ZD03','ZC01','ZC02','ZD02','ZV01','ZV05','ZV08','ZV03','ZV09','ZV04','ZV07','ZV02','ZV06') THEN 'Rechazos'
      ELSE null
    END nivel_1_motivo_desviacion,

    CASE
      WHEN al.cod_motivo_desviacion IN ('ZD06','ZD04','ZD01','ZD07','ZD05','ZD08','ZD03') THEN 'Distribución'
      WHEN al.cod_motivo_desviacion IN ('ZC01','ZC02','ZD02') THEN 'Planeamiento'
      WHEN al.cod_motivo_desviacion IN ('ZV01','ZV05','ZV08','ZV03','ZV09','ZV04','ZV07','ZV02','ZV06') THEN 'Comercial'
    END area_responsable_motivo_desviacion,

    CASE
      WHEN al.cod_motivo_desviacion IN ('ZD06','ZD04','ZD01','ZD07','ZD05','ZD08','ZD03') THEN 'Rechazos por distribución'
      WHEN al.cod_motivo_desviacion IN ('ZC01','ZC02','ZD02') THEN 'Rechazos por calidad del producto'
      WHEN al.cod_motivo_desviacion IN ('ZV01','ZV05','ZV08','ZV03','ZV09') THEN 'Rechazo comercial por inconsistencia de data'
      WHEN al.cod_motivo_desviacion IN ('ZV04','ZV07','ZV02','ZV06') THEN 'Rechazo responsabilidad Cliente'
    END nivel_2_simplificado_desviacion,
    
    fec_factura,
    -- cnt_fill_rate,
    -- fec_fill_rate,
    mnt_peso_neto,
    cod_unidad_medida_peso,
    mnt_neto_pedido,
    cod_moneda,
    factor_peso,  
    factor_valor,
    valor_neto_entrega,
    al.mnt_peso_entrega,
    al.mnt_peso_factura,
    al.valor_factura,
    al.mnt_peso_rechazo,
    al.valor_rechazo,
    al.mnt_peso_desviacion,
    al.mnt_neto_desviacion,
    flg
  FROM  gold_fill_rate_nombres al 
  LEFT JOIN  `{silver_project_id}.slv_modelo_material.material`       ma
  ON    ma.id_material_origen   = al.cod_material_historico
  AND   ma.des_origen           = 'SAPS4'  

  LEFT JOIN  `{silver_project_id}.slv_modelo_material.material`       mkt
  ON ltrim(al.cod_material_actual,'0')  = ltrim(mkt.id_material_origen,'0')
  AND   mkt.des_origen           = 'SAPS4'
  -- ON ltrim(al.cod_material_actual,'0')  = ltrim(mkt.matnr,'0')
  -- AND mkt.spras                         = 'S'

  LEFT JOIN `{silver_project_id}.slv_modelo_maestro.centros` os
  ON    os.cod_centro           = al.cod_centro
  AND   os.des_origen           = 'SAPS4'

  LEFT JOIN `{silver_project_id}.slv_modelo_material.material_jerarquia` a
  ON   a.cod_jerarquia_material = al.cod_presentacion

  INNER JOIN `{silver_project_id}.slv_modelo_maestro.descripcion` ts
  ON    ts.nom_tabla            = 'TSPAT'
  AND   ts.cod_descripcion      = al.cod_sector_comercial

  INNER JOIN `{silver_project_id}.slv_modelo_maestro.descripcion` tt
  ON    tt.nom_tabla            = 'T005T'
  AND   tt.cod_descripcion      = al.cod_pais

  INNER JOIN `{silver_project_id}.slv_modelo_maestro.descripcion` tb
  ON    tb.nom_tabla            = 'TVKBT'
  AND   tb.cod_descripcion      = al.cod_oficina_venta

  LEFT JOIN `{silver_project_id}.slv_modelo_interlocutor.interlocutor` tor
  ON    tor.des_origen          = 'SAPS4'
  AND   tor.id_interlocutor_origen = al.cod_cliente

  INNER JOIN `{silver_project_id}.slv_modelo_maestro.descripcion` t5
  ON    t5.nom_tabla            = 'T151T'
  AND   t5.cod_descripcion      = al.cod_grupo_cliente

  INNER JOIN `{silver_project_id}.slv_modelo_maestro.descripcion` t8
  ON    t8.nom_tabla            = 'T188T'
  AND   t8.cod_descripcion      = al.cod_grupo_precio

  INNER JOIN `{silver_project_id}.slv_modelo_maestro.descripcion` t7
  ON    t7.nom_tabla            = 'T171T'
  AND   t7.cod_descripcion      = al.cod_zona_cliente

  LEFT JOIN `{silver_project_id}.slv_modelo_maestro.descripcion` tm
  ON    tm.nom_tabla            = 'TVM1T'
  AND   tm.cod_descripcion      = al.cod_negocio

  LEFT JOIN `{silver_project_id}.slv_modelo_maestro.descripcion` t0
  ON    t0.nom_tabla            = 'T001'
  AND   t0.cod_descripcion      = al.cod_sociedad_factura

  LEFT JOIN `{silver_project_id}.slv_modelo_maestro.descripcion` t00
  ON    t00.nom_tabla            = 'T189T'
  AND   t00.cod_descripcion      = al.cod_categoria_precio

  LEFT JOIN `{silver_project_id}.slv_modelo_maestro.descripcion` tw
  ON    tw.nom_tabla            = 'TVTWT'
  AND   tw.cod_descripcion      = al.cod_canal_distribucion

  LEFT JOIN `{silver_project_id}.slv_modelo_maestro.descripcion` tk
  ON    tk.nom_tabla            = 'TVKOT'
  AND   tk.cod_descripcion      = al.cod_organizacion_venta

  LEFT JOIN `{silver_project_id}.slv_modelo_maestro.descripcion` ctv
  ON    ctv.nom_tabla            = 'TVGRT'
  AND   ctv.cod_descripcion      = al.cod_grupo_vendedor

  LEFT JOIN `{silver_project_id}.slv_modelo_maestro.descripcion` ag
  ON    ag.nom_tabla            = 'TVAGT'
  AND   ag.cod_descripcion      = al.cod_motivo_rechazo 

  LEFT JOIN `{silver_project_id}.slv_modelo_interlocutor.interlocutor` ti
  ON    ti.des_origen          = 'SAPS4'
  AND   ti.id_interlocutor_origen = al.cod_interlocutor_destino

  LEFT JOIN `{silver_project_id}.slv_modelo_interlocutor.interlocutor` lo
  ON    lo.des_origen          = 'SAPS4'
  AND   lo.id_interlocutor_origen = al.cod_interlocutor_territorio

  LEFT JOIN `{silver_project_id}.slv_modelo_maestro.descripcion` vz
  ON    vz.nom_tabla            = 'TVZBT'
  AND   vz.cod_descripcion      = al.cod_condicion_pago 

  LEFT JOIN `{silver_project_id}.slv_modelo_maestro.descripcion`  vs
  ON    vs.nom_tabla            = 'TVSBT'
  AND   vs.cod_descripcion      = al.cod_condicion_expedicion

  LEFT JOIN `{silver_project_id}.slv_modelo_maestro.descripcion` ste
  ON    ste.nom_tabla            = 'TVSTT'
  AND   ste.cod_descripcion      = al.cod_punto_expedicion

  LEFT JOIN `{silver_project_id}.slv_modelo_maestro.descripcion` re
  ON    re.nom_tabla            = 'T005U'
  AND   re.cod_descripcion      = al.cod_region

  LEFT JOIN `{silver_project_id}.slv_modelo_maestro.descripcion` vls
  ON    vls.nom_tabla            = 'TVLST'
  AND   vls.cod_descripcion      = al.cod_bloqueo_entrega

  LEFT JOIN `{silver_project_id}.slv_modelo_maestro.descripcion` pod
  ON    pod.nom_tabla            = 'TVPODGT'
  AND   pod.cod_descripcion      = al.cod_motivo_desviacion

  LEFT JOIN `{silver_project_id}.slv_modelo_maestro.descripcion` gg
  ON    gg.nom_tabla            = 'TVKGGT'
  AND   gg.cod_descripcion      = al.cod_grupo_condicion

  LEFT JOIN `{silver_project_id}.slv_modelo_maestro.descripcion`  ak
  ON    ak.nom_tabla            = 'TVAKT'
  AND   ak.cod_descripcion      = al.cod_clase_pedido
  --cod_motivo_rechazo_del_rechazo --- descomentar cuando se tenga el campo
  LEFT JOIN `{silver_project_id}.slv_modelo_maestro.descripcion` vau
  ON    vau.nom_tabla            = 'TVAUT'
  AND   vau.cod_descripcion      = al.cod_motivo_rechazo_del_rechazo

  LEFT JOIN `{silver_project_id}.slv_modelo_maestro.descripcion` tma
  ON    tma.nom_tabla            = 'TVM4T'
  AND   tma.cod_descripcion      = al.cod_marca

  LEFT JOIN `{silver_project_id}.slv_modelo_interlocutor.interlocutor` jer_1
  ON    jer_1.des_origen          = 'SAPS4'
  AND   jer_1.id_interlocutor_origen = al.cod_jerarquia_nivel_1

  LEFT JOIN `{silver_project_id}.slv_modelo_interlocutor.interlocutor` jer_2
  ON    jer_2.des_origen          = 'SAPS4'
  AND   jer_2.id_interlocutor_origen = al.cod_jerarquia_nivel_2)
  
  WHERE fec_creacion BETWEEN CAST(var_fecha_inicio_insert as date) AND CAST(var_fecha_actual as date)  -- Filtro para tomar solo el rango de fechas para insercion
;
/************FIN AGREGAR DESCRIPCIONES*************/


/*********************CORREGIR LOS CASOS DE UNA ENTREGA RELACIONADA A MAS DE UN DOC DE RECHAZO***************************/
CREATE OR REPLACE TEMP TABLE CORREGIR_ENTREGA_RECHAZO AS
  SELECT 
    cod_pedido_venta,
    num_correlativo_pedido,
    entrega,
    posicion_entrega
  FROM gold_fill_rate
  WHERE flg in (1,3)
  GROUP BY
    cod_pedido_venta,
    num_correlativo_pedido,
    entrega,
    posicion_entrega
  HAVING count(*)>1
;


CREATE OR REPLACE TEMP TABLE gold_fill_rate_alterno AS
  SELECT 
    f.*
  FROM gold_fill_rate f
  INNER JOIN CORREGIR_ENTREGA_RECHAZO d
  ON f.cod_pedido_venta           = d.cod_pedido_venta
    AND f.num_correlativo_pedido  = d.num_correlativo_pedido
    AND f.entrega                 = d.entrega
    AND f.posicion_entrega        = d.posicion_entrega
;


CREATE OR REPLACE TEMP TABLE gold_fill_rate_regular AS
  SELECT 
    f.*
  FROM gold_fill_rate f
  LEFT JOIN CORREGIR_ENTREGA_RECHAZO d
  ON f.cod_pedido_venta           = d.cod_pedido_venta
    AND f.num_correlativo_pedido  = d.num_correlativo_pedido
    AND f.entrega                 = d.entrega
    AND f.posicion_entrega        = d.posicion_entrega
  WHERE d.cod_pedido_venta IS NULL
    AND d.num_correlativo_pedido IS NULL
    AND d.entrega IS NULL
    AND d.posicion_entrega IS NULL
;


-- INICIO DELIVERY FLUJO ALTERNO
CREATE OR REPLACE TEMP TABLE delivery_fill_rate_alterno AS
  WITH suma_cantidad AS 
  (
    SELECT 
      cod_pedido_venta,
      num_correlativo_pedido,
      MAX(fec_factura) fec_factura,
      MAX(fec_rechazo_factura) fec_rechazo_factura, 
      MAX(fec_pedido) fec_pedido, 
      MAX(fec_rechazo) fec_rechazo,
      --SUM(cnt_entregada_unidad_venta) AS cnt_entregada_unidad_venta,SUM(cnt_material) AS cnt_material, sum(cnt_total_desviacion) as cnt_total_desviacion,
      SUM(valor_neto_entrega)           valor_neto_entrega,  
      SUM(cnt_rechazada)                cnt_rechazada,
      SUM(mnt_peso_entrega)             mnt_peso_entrega, /**/
      SUM(mnt_peso_factura)             mnt_peso_factura, /**/
      SUM(valor_factura)                valor_factura,
      SUM(mnt_peso_rechazo)             mnt_peso_rechazo,
      SUM(mnt_peso_desviacion)          mnt_peso_desviacion,
      SUM(mnt_neto_desviacion)          mnt_neto_desviacion,
      SUM(valor_rechazo)                valor_rechazo
    FROM gold_fill_rate_alterno
    GROUP BY 1,2
  )
  SELECT 
    DISTINCT
    f.mnt_peso_neto,
    f.cod_unidad_medida_peso,
    f.mnt_neto_pedido,
    f.cod_moneda,
    b.valor_neto_entrega,
    f.factor_peso,  
    f.factor_valor, 
    b.mnt_peso_entrega,
    b.mnt_peso_factura,
    b.valor_factura,
    b.mnt_peso_rechazo,
    b.valor_rechazo,
    f.cod_sociedad_factura,
    f.des_sociendad_factura,
    f.cod_organizacion_venta,
    f.des_organizacion_venta,
    f.cod_canal_distribucion,
    f.des_canal_distribucion,
    f.cod_sector_comercial,
    f.des_sector_comercial,
    f.cod_cliente,
    f.nom_interlocutor,
    f.cod_jerarquia_nivel_1,
    f.des_jerarquia_nivel_1,
    f.cod_jerarquia_nivel_2,
    f.des_jerarquia_nivel_2,
    f.cod_interlocutor_destino,
    f.nom_interlocutor_destino,
    f.cod_pedido_venta,
    f.cod_clase_pedido,
    f.des_clase_pedido,
    f.cod_condicion_pago,
    f.des_condicion_pago,
    f.cod_bloqueo_entrega,
    f.des_bloqueo_entrega,
    f.cod_condicion_expedicion,
    f.des_condicion_expedicion,
    f.num_correlativo_pedido,
    f.cod_material_historico,
    f.des_material, /*A*/
    f.cod_material_actual,
    f.des_material_actual,
    f.cod_unidad_comercial,
    f.cnt_pedido,
    f.fec_creacion,
    f.cod_motivo_rechazo,
    f.des_motivo_rechazo,
    f.cod_centro,
    f.des_centro, /*A*/
    f.cod_punto_expedicion,
    f.des_punto_expedicion,
    f.cod_pais,
    f.des_pais,
    f.cod_region,
    f.des_region,
    f.des_poblacion,
    f.des_distrito,
    f.cod_zona_cliente,
    f.des_zona_cliente,
    f.cod_oficina_venta,
    f.des_oficina_venta,
    f.cod_grupo_cliente,
    f.des_grupo_cliente,
    f.cod_segmento_cliente,
    f.cod_grupo_condicion,
    f.des_grupo_condicion,
    f.cod_grupo_precio,
    f.des_grupo_precio,
    f.cod_grupo_vendedor,
    f.des_grupo_vendedor,
    f.cod_categoria_precio,
    f.des_categoria_precio,
    f.cod_marca,
    f.des_marca,
    f.COD_DUENIO_MARCA,
    f.DES_DUENIO_MARCA,
    f.cod_plataforma,
    f.des_plataforma,
    f.cod_sub_plataforma,
    f.des_sub_plataforma,
    f.cod_categoria,
    f.des_categoria,
    f.cod_familia,
    f.des_familia,
    f.cod_variedad,
    f.des_variedad,
    f.cod_presentacion,/*observado*/
    f.cod_negocio,
    f.des_negocio,
    f.cod_subnegocio,
    f.cod_tipo_material,
    f.des_tipo_material, /*A*/
    f.cod_interlocutor_territorio,
    f.nom_interlocutor_territorio,
    f.nivel_2_simplificado_rechazo,
    f.nivel_1_motivo_rechazo,
    f.area_responsable_motivo_rechazo,
    b.fec_rechazo,
    f.cnt_entregada_unidad_venta,
    f.cnt_total_desviacion,
    f.cnt_desviacion_total,
    IFNULL(f.cnt_material,0) cnt_material,
    b.cnt_rechazada,
    (((f.cnt_material - IFNULL(b.cnt_rechazada, 0))/f.cnt_pedido)*100) AS cnt_fill_rate,
    GREATEST(IFNULL(b.fec_factura, '1900-01-01'),IFNULL(b.fec_rechazo_factura, '1900-01-01'), IFNULL(b.fec_pedido, '1900-01-01'), IFNULL(b.fec_rechazo, '1900-01-01')) AS fec_fill_rate,
    b.mnt_peso_desviacion,
    b.mnt_neto_desviacion,
    f.flg
  FROM gold_fill_rate_alterno f
  INNER JOIN  suma_cantidad b
  ON f.cod_pedido_venta           = b.cod_pedido_venta
    AND f.num_correlativo_pedido  = b.num_correlativo_pedido
;


-- INICIO DELIVERY FLUJO REGULAR
CREATE OR REPLACE TEMP TABLE delivery_fill_rate_regular AS
  WITH suma_cantidad AS 
  (
    SELECT 
      cod_pedido_venta, 
      num_correlativo_pedido,
      MAX(fec_factura)          fec_factura,
      MAX(fec_rechazo_factura)  fec_rechazo_factura, 
      MAX(fec_pedido)           fec_pedido, 
      MAX(fec_rechazo)          fec_rechazo,

      SUM(cnt_entregada_unidad_venta)     cnt_entregada_unidad_venta,
      SUM(valor_neto_entrega)             valor_neto_entrega,
      SUM(cnt_material)                   cnt_material, 
      SUM(cnt_rechazada)                  cnt_rechazada, 
      SUM(cnt_total_desviacion)           cnt_total_desviacion,
      SUM(cnt_desviacion_total)           cnt_desviacion_total,
      SUM(mnt_peso_entrega)               mnt_peso_entrega, /**/
      SUM(mnt_peso_factura)               mnt_peso_factura, /**/
      SUM(valor_factura)                  valor_factura,
      SUM(mnt_peso_rechazo)               mnt_peso_rechazo,
      SUM(valor_rechazo)                  valor_rechazo,
      SUM(mnt_peso_desviacion)            mnt_peso_desviacion,
      SUM(mnt_neto_desviacion)            mnt_neto_desviacion,
    FROM gold_fill_rate_regular
    GROUP BY 1,2
  )
  SELECT
    DISTINCT
        /* Datos Agregados*/
    f.mnt_peso_neto,
    f.cod_unidad_medida_peso,
    f.mnt_neto_pedido,
    f.cod_moneda,
    b.valor_neto_entrega,
    f.factor_peso,  
    f.factor_valor, 
    b.mnt_peso_entrega,
    b.mnt_peso_factura,
    b.valor_factura,
    b.mnt_peso_rechazo,
    b.valor_rechazo,
    /**/
    f.cod_sociedad_factura,
    f.des_sociendad_factura,
    f.cod_organizacion_venta,
    f.des_organizacion_venta,
    f.cod_canal_distribucion,
    f.des_canal_distribucion,
    f.cod_sector_comercial,
    f.des_sector_comercial,
    f.cod_cliente,
    f.nom_interlocutor,
    f.cod_jerarquia_nivel_1,
    f.des_jerarquia_nivel_1,
    f.cod_jerarquia_nivel_2,
    f.des_jerarquia_nivel_2,
    f.cod_interlocutor_destino,
    f.nom_interlocutor_destino,
    f.cod_pedido_venta,
    f.cod_clase_pedido,
    f.des_clase_pedido,
    f.cod_condicion_pago,
    f.des_condicion_pago,
    f.cod_bloqueo_entrega,
    f.des_bloqueo_entrega,
    f.cod_condicion_expedicion,
    f.des_condicion_expedicion,
    f.num_correlativo_pedido,
    f.cod_material_historico,
    f.des_material, /*A*/
    f.cod_material_actual,
    f.des_material_actual,
    f.cod_unidad_comercial,
    f.cnt_pedido,
    f.fec_creacion,
    f.cod_motivo_rechazo,
    f.des_motivo_rechazo,
    f.cod_centro,
    f.des_centro, /*A*/
    f.cod_punto_expedicion,
    f.des_punto_expedicion,
    f.cod_pais,
    f.des_pais,
    f.cod_region,
    f.des_region,
    f.des_poblacion,
    f.des_distrito,
    f.cod_zona_cliente,
    f.des_zona_cliente,
    f.cod_oficina_venta,
    f.des_oficina_venta,
    f.cod_grupo_cliente,
    f.des_grupo_cliente,
    f.cod_segmento_cliente,
    f.cod_grupo_condicion,
    f.des_grupo_condicion,
    f.cod_grupo_precio,
    f.des_grupo_precio,
    f.cod_grupo_vendedor,
    f.des_grupo_vendedor,
    f.cod_categoria_precio,
    f.des_categoria_precio,
    f.cod_marca,
    f.des_marca,
    f.COD_DUENIO_MARCA,
    f.DES_DUENIO_MARCA,
    f.cod_plataforma,
    f.des_plataforma,
    f.cod_sub_plataforma,
    f.des_sub_plataforma,
    f.cod_categoria,
    f.des_categoria,
    f.cod_familia,
    f.des_familia,
    f.cod_variedad,
    f.des_variedad,
    f.cod_presentacion,/*observado*/
    f.cod_negocio,
    f.des_negocio,
    f.cod_subnegocio,
    f.cod_tipo_material,
    f.des_tipo_material, /*A*/
    f.cod_interlocutor_territorio,
    f.nom_interlocutor_territorio,
    f.nivel_2_simplificado_rechazo,
    f.nivel_1_motivo_rechazo,
    f.area_responsable_motivo_rechazo,
    b.fec_rechazo,
    b.cnt_entregada_unidad_venta,
    b.cnt_total_desviacion,
    b.cnt_desviacion_total,
    IFNULL(b.cnt_material,0)                                              AS cnt_material,
    b.cnt_rechazada,
    (((b.cnt_material - IFNULL(b.cnt_rechazada,0))/f.cnt_pedido)*100)     AS cnt_fill_rate,
    GREATEST(IFNULL(b.fec_factura, '1900-01-01'),IFNULL(b.fec_rechazo_factura, '1900-01-01'), IFNULL(b.fec_pedido, '1900-01-01'), IFNULL(b.fec_rechazo, '1900-01-01')) AS fec_fill_rate,
    b.mnt_peso_desviacion,
    b.mnt_neto_desviacion,
    f.flg
  FROM gold_fill_rate_regular f
  INNER JOIN  suma_cantidad b
  ON f.cod_pedido_venta           = b.cod_pedido_venta
    AND f.num_correlativo_pedido  = b.num_correlativo_pedido
;
-- FIN DELIVERY


-- UNION DE LOS 2 FLUJOS DE DELIVERY
CREATE OR REPLACE TEMP TABLE delivery_fill_rate_union AS
  SELECT * FROM delivery_fill_rate_regular
  UNION ALL
  SELECT * FROM delivery_fill_rate_alterno
;


CREATE OR REPLACE TEMP TABLE delivery_fill_rate_agrupado AS
  SELECT
    f.cod_sociedad_factura,
    f.des_sociendad_factura,
    f.cod_organizacion_venta,
    f.des_organizacion_venta,
    f.cod_canal_distribucion,
    f.des_canal_distribucion,
    f.cod_sector_comercial,
    f.des_sector_comercial,
    f.cod_cliente,
    f.nom_interlocutor,
    f.cod_jerarquia_nivel_1,
    f.des_jerarquia_nivel_1,
    f.cod_jerarquia_nivel_2,
    f.des_jerarquia_nivel_2,
    f.cod_interlocutor_destino,
    f.nom_interlocutor_destino,
    f.cod_pedido_venta,
    f.cod_clase_pedido,
    f.des_clase_pedido,
    f.cod_condicion_pago,
    f.des_condicion_pago,
    f.cod_bloqueo_entrega,
    f.des_bloqueo_entrega,
    f.cod_condicion_expedicion,
    f.des_condicion_expedicion,
    f.num_correlativo_pedido,
    f.cod_material_historico,
    f.des_material, /*A*/
    f.cod_material_actual,
    f.des_material_actual,
    f.cod_unidad_comercial,
    f.cnt_pedido,
    f.fec_creacion,
    f.cod_motivo_rechazo,
    f.des_motivo_rechazo,
    f.cod_centro,
    f.des_centro, /*A*/
    f.cod_punto_expedicion,
    f.des_punto_expedicion,
    f.cod_pais,
    f.des_pais,
    f.cod_region,
    f.des_region,
    f.des_poblacion,
    f.des_distrito,
    f.cod_zona_cliente,
    f.des_zona_cliente,
    f.cod_oficina_venta,
    f.des_oficina_venta,
    f.cod_grupo_cliente,
    f.des_grupo_cliente,
    f.cod_segmento_cliente,
    f.cod_grupo_condicion,
    f.des_grupo_condicion,
    f.cod_grupo_precio,
    f.des_grupo_precio,
    f.cod_grupo_vendedor,
    f.des_grupo_vendedor,
    f.cod_categoria_precio,
    f.des_categoria_precio,
    f.cod_marca,
    f.des_marca,
    f.COD_DUENIO_MARCA,
    f.DES_DUENIO_MARCA,
    f.cod_plataforma,
    f.des_plataforma,
    f.cod_sub_plataforma,
    f.des_sub_plataforma,
    f.cod_categoria,
    f.des_categoria,
    f.cod_familia,
    f.des_familia,
    f.cod_variedad,
    f.des_variedad,
    f.cod_presentacion,/*observado*/
    f.cod_negocio,
    f.des_negocio,
    f.cod_subnegocio,
    f.cod_tipo_material,
    f.des_tipo_material, /*A*/
    f.cod_interlocutor_territorio,
    f.nom_interlocutor_territorio,
    f.nivel_2_simplificado_rechazo,
    f.nivel_1_motivo_rechazo,
    f.area_responsable_motivo_rechazo,
    MAX(f.fec_rechazo)                  AS fec_rechazo,
    SUM(f.cnt_entregada_unidad_venta)   AS cnt_entregada_unidad_venta,
    SUM(f.cnt_total_desviacion)         AS cnt_total_desviacion,
    SUM(F.cnt_desviacion_total)         AS cnt_desviacion_total,
    SUM(f.cnt_material)                 AS cnt_material,
    (IFNULL(f.cnt_pedido, 0) - IFNULL(SUM(f.cnt_entregada_unidad_venta), 0))  as cnt_rechazada_pedido, ---Se realiza ajuste para realizar calculo en temporal agrupamiento
    SUM(f.cnt_rechazada)                AS cnt_rechazada,
    SUM(f.cnt_fill_rate)                AS cnt_fill_rate,
    MAX(f.fec_fill_rate)                AS fec_fill_rate,
    /* Datos Agregados*/
    f.mnt_peso_neto,
    f.cod_unidad_medida_peso,
    f.mnt_neto_pedido,
    f.cod_moneda,
    SUM(f.valor_neto_entrega)           valor_neto_entrega,
    f.factor_peso,
    f.factor_valor,
    SUM(f.mnt_peso_entrega)             mnt_peso_entrega,
    SUM(f.mnt_peso_factura)             mnt_peso_factura,
    SUM(f.valor_factura)                valor_factura,
    SUM(f.mnt_peso_rechazo)             mnt_peso_rechazo,
    SUM(f.valor_rechazo)                valor_rechazo,
    SUM(f.mnt_peso_desviacion)          mnt_peso_desviacion,
    SUM(f.mnt_neto_desviacion)          mnt_neto_desviacion,
    f.flg
  FROM delivery_fill_rate_union f
  GROUP BY
  f.cod_sociedad_factura,
  f.des_sociendad_factura,
  f.cod_organizacion_venta,
  f.des_organizacion_venta,
  f.cod_canal_distribucion,
  f.des_canal_distribucion,
  f.cod_sector_comercial,
  f.des_sector_comercial,
  f.cod_cliente,
  f.nom_interlocutor,
  f.cod_jerarquia_nivel_1,
  f.des_jerarquia_nivel_1,
  f.cod_jerarquia_nivel_2,
  f.des_jerarquia_nivel_2,
  f.cod_interlocutor_destino,
  f.nom_interlocutor_destino,
  f.cod_pedido_venta,
  f.cod_clase_pedido,
  f.des_clase_pedido,
  f.cod_condicion_pago,
  f.des_condicion_pago,
  f.cod_bloqueo_entrega,
  f.des_bloqueo_entrega,
  f.cod_condicion_expedicion,
  f.des_condicion_expedicion,
  f.num_correlativo_pedido,
  f.cod_material_historico,
  f.des_material, /*A*/
  f.cod_material_actual,
  f.des_material_actual,
  f.cod_unidad_comercial,
  f.cnt_pedido,
  f.fec_creacion,
  f.cod_motivo_rechazo,
  f.des_motivo_rechazo,
  f.cod_centro,
  f.des_centro, /*A*/
  f.cod_punto_expedicion,
  f.des_punto_expedicion,
  f.cod_pais,
  f.des_pais,
  f.cod_region,
  f.des_region,
  f.des_poblacion,
  f.des_distrito,
  f.cod_zona_cliente,
  f.des_zona_cliente,
  f.cod_oficina_venta,
  f.des_oficina_venta,
  f.cod_grupo_cliente,
  f.des_grupo_cliente,
  f.cod_segmento_cliente,
  f.cod_grupo_condicion,
  f.des_grupo_condicion,
  f.cod_grupo_precio,
  f.des_grupo_precio,
  f.cod_grupo_vendedor,
  f.des_grupo_vendedor,
  f.cod_categoria_precio,
  f.des_categoria_precio,
  f.cod_marca,
  f.des_marca,
  f.COD_DUENIO_MARCA,
  f.DES_DUENIO_MARCA,
  f.cod_plataforma,
  f.des_plataforma,
  f.cod_sub_plataforma,
  f.des_sub_plataforma,
  f.cod_categoria,
  f.des_categoria,
  f.cod_familia,
  f.des_familia,
  f.cod_variedad,
  f.des_variedad,
  f.cod_presentacion,/*observado*/
  f.cod_negocio,
  f.des_negocio,
  f.cod_subnegocio,
  f.cod_tipo_material,
  f.des_tipo_material, /*A*/
  f.cod_interlocutor_territorio,
  f.nom_interlocutor_territorio,
  f.nivel_2_simplificado_rechazo,
  f.nivel_1_motivo_rechazo,
  f.area_responsable_motivo_rechazo,
  f.mnt_peso_neto,
  f.cod_unidad_medida_peso,
  f.mnt_neto_pedido,
  f.cod_moneda,
  f.factor_peso,
  f.factor_valor,
  f.flg
;


/**************************CORRECCIÓN DE LOS CASOS ZPAN********************************/
--TRUNCATE TABLE `{golden_project_id}.gld_cliente.s4_fill_rate`; 
CREATE OR REPLACE TABLE `{golden_project_id}.gld_cliente.s4_fill_rate`; 
--INSERT  INTO   `{golden_project_id}.gld_cliente.s4_fill_rate`

SELECT
  CAST(a.fec_creacion AS DATE) AS periodo,
  a.id_documento_entrega,
  a.posicion_entrega        AS num_correlativo_entrega_modelo,
  a.cod_documento_modelo,
  a.num_correlativo_documento_modelo,
  a.entrega                 AS cod_documento_entrega,
  a.num_correlativo_lips    AS num_correlativo_entrega,
  a.factura                 AS cod_documento_venta,
  a.posicion_factura        AS num_correlativo_venta,
  a.clase_factura           AS cod_clase_documento,
  a.documento_rechazo       AS cod_pedido_rechazo,
  a.posicion_rechazo        AS num_correlativo_pedido_rechazo,
  a.cod_sociedad_factura,
  a.des_sociendad_factura   AS des_sociedad,
  a.cod_organizacion_venta,
  a.des_organizacion_venta,
  a.cod_canal_distribucion,
  a.des_canal_distribucion,
  a.cod_sector_comercial,
  a.des_sector_comercial,
  a.cod_cliente,
  a.nom_interlocutor,
  a.cod_jerarquia_nivel_1,
  a.des_jerarquia_nivel_1,
  a.cod_jerarquia_nivel_2,
  a.des_jerarquia_nivel_2,
  a.COD_DUENIO_MARCA            AS cod_duenio_marca,
  a.DES_DUENIO_MARCA            AS des_duenio_marca,
  a.cod_interlocutor_destino,
  a.nom_interlocutor_destino,
  a.cod_pedido_venta,
  a.cod_clase_pedido,
  a.des_clase_pedido,
  a.cod_condicion_pago,
  a.des_condicion_pago,
  a.cod_bloqueo_entrega,
  a.des_bloqueo_entrega,
  a.cod_condicion_expedicion,
  a.des_condicion_expedicion,
  a.num_correlativo_pedido,
  a.cod_material_historico,
  a.des_material                AS des_material_historico,
  a.cod_material_actual,
  a.des_material_actual,
  a.cod_tipo_material,
  a.des_tipo_material,
  IFNULL(a.cnt_material,0)      AS cnt_material,
  a.cod_marca,
  a.des_marca,
  a.cod_plataforma,
  a.des_plataforma,
  a.cod_sub_plataforma,
  a.des_sub_plataforma,
  a.cod_categoria,
  a.des_categoria,
  a.cod_familia,
  a.des_familia,
  a.cod_variedad,
  a.des_variedad,
  a.cod_presentacion,
  a.cod_unidad_comercial,
  IFNULL(a.cnt_pedido,0)        AS cnt_pedido,
  a.fec_creacion,
  a.fec_rechazo_factura         AS fec_factura_rechazo,

  CASE 
    WHEN a.cod_pedido_venta         = b.cod_pedido_venta 
      AND a.num_correlativo_pedido  = b.num_correlativo_pedido 
      AND b.cnt_rechazada_pedido    <>  0 
      AND b.cod_clase_pedido        = 'ZPAN' 
      AND b.flg                     = 3 
      AND a.cod_motivo_rechazo IS NULL
    THEN '51' 
    ELSE a.cod_motivo_rechazo 
  END cod_motivo_rechazo,

  CASE 
    WHEN a.cod_pedido_venta         = b.cod_pedido_venta 
      AND a.num_correlativo_pedido  = b.num_correlativo_pedido 
      AND b.cnt_rechazada_pedido    <> 0 
      AND b.cod_clase_pedido        = 'ZPAN' 
      AND b.flg                     = 3 
      AND a.cod_motivo_rechazo IS NULL 
    THEN 'NO ATENDIDO FALTA STOCK PLANEAMIENTO' 
    ELSE a.des_motivo_rechazo 
  END des_motivo_rechazo,

  a.fec_rechazo             AS fec_pedido_rechazo,
  a.cod_centro,
  a.des_centro,
  a.cod_punto_expedicion,
  a.des_punto_expedicion,
  a.cod_pais,
  a.des_pais,
  a.cod_region,
  a.des_region,
  a.des_poblacion,
  a.des_distrito,
  a.cod_zona_cliente,
  a.des_zona_cliente,
  a.cod_oficina_venta,
  a.des_oficina_venta,
  a.cod_grupo_cliente,
  a.des_grupo_cliente,
  a.cod_segmento_cliente,
  a.cod_grupo_condicion,
  a.des_grupo_condicion,
  a.cod_grupo_precio,
  a.des_grupo_precio,
  a.cod_grupo_vendedor,
  a.des_grupo_vendedor,
  a.cod_categoria_precio,
  a.des_categoria_precio,
  a.cod_negocio,
  a.des_negocio,
  a.cod_subnegocio,
  IFNULL(a.cnt_entregada_unidad_venta,0)  AS cnt_entregada_unidad_venta,
  IFNULL(a.cnt_total_desviacion,0)        AS cnt_total_desviacion,
  IFNULL(a.cnt_desviacion_total,0)        AS cnt_desviacion_total,
  a.cod_motivo_desviacion,
  a.des_motivo_desviacion,
  a.fec_pedido,
  a.cod_interlocutor_territorio,
  a.nom_interlocutor_territorio,
  a.cod_motivo_rechazo_del_rechazo        AS cod_motivo_rechazo_pedido_rechazo,
  a.des_motivo_rechazo_del_rechazo        AS des_motivo_rechazo_pedido_rechazo,
  IFNULL(a.cnt_rechazada,0)               AS cnt_rechazo,

  CASE 
    WHEN a.cod_pedido_venta         = b.cod_pedido_venta 
      AND a.num_correlativo_pedido  = b.num_correlativo_pedido 
      AND b.cnt_rechazada_pedido    <>  0 
      AND b.cod_clase_pedido        = 'ZPAN' 
      AND b.flg                     = 3 
      AND a.cod_motivo_rechazo IS NULL 
    THEN 'Disponibilidad de inventario' 
    ELSE a.nivel_1_motivo_rechazo 
  END des_motivo_rechazo_nivel_1,

  CASE 
    WHEN a.cod_pedido_venta         = b.cod_pedido_venta 
      AND a.num_correlativo_pedido  = b.num_correlativo_pedido 
      AND b.cnt_rechazada_pedido    <>  0 
      AND b.cod_clase_pedido        = 'ZPAN' 
      AND b.flg                     = 3 
      AND a.cod_motivo_rechazo IS NULL 
    THEN 'Planeamiento' 
    ELSE a.area_responsable_motivo_rechazo 
  END nom_area_responsable_rechazo,

  CASE 
    WHEN a.cod_pedido_venta         = b.cod_pedido_venta 
      AND a.num_correlativo_pedido  = b.num_correlativo_pedido 
      AND b.cnt_rechazada_pedido    <>  0 
      AND b.cod_clase_pedido        = 'ZPAN' 
      AND b.flg                     = 3 
      AND a.cod_motivo_rechazo IS NULL 
    THEN 'No atendido falta stock planeamiento' 
    ELSE a.nivel_2_simplificado_rechazo 
  END des_motivo_rechazo_nivel_2,

  a.nivel_1_motivo_desviacion                   AS des_motivo_desviacion_nivel_1,
  a.area_responsable_motivo_desviacion          AS nom_area_responsable_desviacion,
  a.nivel_2_simplificado_desviacion             AS des_motivo_desviacion_nivel_2,
  a.nivel_1_motivo_rechazo_del_rechazo          AS des_motivo_rechazo_pedido_rechazo_nivel_1,
  a.nivel_2_simplificado_rechazo_del_rechazo    AS des_motivo_rechazo_pedido_rechazo_nivel_2,
  a.area_responsable_motivo_rechazo_del_rechazo AS nom_area_responsable_pedido_rechazo,
  a.fec_factura                                 AS fec_documento,
  IFNULL(a.mnt_peso_neto,0)                     AS mnt_peso_neto,
  a.cod_unidad_medida_peso,
  IFNULL(a.mnt_neto_pedido,0)                   AS mnt_neto_pedido,
  a.cod_moneda,
  IFNULL(a.valor_neto_entrega,0)                AS mnt_neto_entrega,
  IFNULL(a.factor_peso,0)                       AS num_factor_peso,
  IFNULL(a.factor_valor,0)                      AS num_factor,
  IFNULL(a.mnt_peso_entrega,0)                  AS mnt_peso_entrega,
  IFNULL(a.mnt_peso_factura,0)                  AS mnt_peso_factura,
  IFNULL(a.valor_factura,0)                     AS mnt_total_factura,
  IFNULL(a.mnt_peso_rechazo,0)                  AS mnt_peso_rechazo,
  IFNULL(a.valor_rechazo,0)                     AS mnt_total_rechazo,
  gt.des_canal,
  gt.cod_negocio_agrupado                       AS cod_negocio_grupo_vendedor,
  a.flg                                         AS ind_bloque_tabla,
  IFNULL(a.mnt_peso_desviacion,0)               AS mnt_peso_desviacion,
  IFNULL(a.mnt_neto_desviacion,0)               AS mnt_neto_desviacion,
  current_datetime('America/Lima')              AS fec_proceso
FROM gold_fill_rate a
LEFT JOIN delivery_fill_rate_agrupado b
ON a.cod_pedido_venta           = b.cod_pedido_venta
  AND a.num_correlativo_pedido  = b.num_correlativo_pedido
  AND b.cnt_rechazada_pedido    <>  0 
  AND b.cod_clase_pedido        = 'ZPAN'
  AND b.flg                     = 3
LEFT JOIN `{silver_project_id}.slv_modelo_maestro.grupo_vendedor_canal_negocio`  gt
ON ltrim(a.cod_grupo_vendedor,'0')          = gt.cod_grupo_vendedor
  AND a.cod_negocio                         = gt.cod_negocio
  AND a.cod_oficina_venta                   = gt.cod_oficina_venta
;
END
