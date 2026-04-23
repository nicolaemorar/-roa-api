require('dotenv').config()

const express = require('express')
const cors = require('cors')
const { Pool } = require('pg')

const app = express()
const port = process.env.PORT || 3001

app.use(cors())
app.use(express.json())

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: {
    rejectUnauthorized: false,
  },
})

app.get('/api/health', async (req, res) => {
  try {
    const result = await pool.query('SELECT NOW() AS server_time')
    res.json({
      ok: true,
      server_time: result.rows[0].server_time,
    })
  } catch (error) {
    console.error('Health check error:', error)
    res.status(500).json({
      ok: false,
      error: 'Database connection failed',
    })
  }
})

app.get('/api/harta/judete', async (req, res) => {
  try {
    const result = await pool.query(`
      SELECT
        d.cod_judet,
        d.nume_judet,
        d.total_puncte,
        d.puncte_de_inceput,
        d.puncte_in_avizare,
        d.puncte_eliminate_igpr,
        d.total_t17,
        d.total_pv_uat,
        d.puncte_eligibile_montaj,
        d.puncte_montate,
        d.puncte_ramase_montaj,
        d.procent_in_avizare,
        d.procent_eliminat_igpr,

        a.dr_igpr_status,
        a.ipj_status,
        a.ipj_sig_circ_status,
        a.cnair_status,
        a.cj_status,
        a.uat_total,
        a.uat_cu_aviz,
        a.uat_cu_cerere,
        a.uat_cu_clarificari,
        a.uat_nesolicitate,

        c.cost_materiale,
        c.cost_manopera,
        c.cost_total,

        f.nr_obiective,
        f.venit_total,
        f.diferenta_venit_cost,
        f.situatie_decontare_rute

      FROM v_dashboard_judet_52 d
      LEFT JOIN v_dashboard_avize_judet_executiv a
        ON a.cod_judet = d.cod_judet
      LEFT JOIN v_cost_standard_judet c
        ON c.cod_judet = d.cod_judet
      LEFT JOIN v_financiar_judet_executiv f
        ON f.cod_judet = d.cod_judet
      ORDER BY d.total_puncte DESC, d.cod_judet
    `)

    res.json(result.rows)
  } catch (error) {
    console.error('GET /api/harta/judete error:', error)
    res.status(500).json({
      error: 'Failed to fetch county dashboard',
    })
  }
})

app.get('/api/harta/judete/:cod_judet', async (req, res) => {
  const { cod_judet } = req.params

  try {
    const summaryResult = await pool.query(
      `
      SELECT
        d.*,
        a.dr_igpr_status,
        a.ipj_status,
        a.ipj_sig_circ_status,
        a.cnair_status,
        a.cj_status,
        a.uat_total,
        a.uat_cu_aviz,
        a.uat_cu_cerere,
        a.uat_cu_clarificari,
        a.uat_nesolicitate,
        c.cost_materiale,
        c.cost_manopera,
        c.cost_total,
        f.nr_obiective,
        f.venit_total,
        f.diferenta_venit_cost,
        f.situatie_decontare_rute
      FROM v_dashboard_judet_52 d
      LEFT JOIN v_dashboard_avize_judet_executiv a
        ON a.cod_judet = d.cod_judet
      LEFT JOIN v_cost_standard_judet c
        ON c.cod_judet = d.cod_judet
      LEFT JOIN v_financiar_judet_executiv f
        ON f.cod_judet = d.cod_judet
      WHERE d.cod_judet = $1
      `,
      [cod_judet]
    )

    const routesResult = await pool.query(
      `
      SELECT *
      FROM v_dashboard_ruta_judet_52
      WHERE cod_judet = $1
      ORDER BY total_puncte DESC, cod_ruta
      `,
      [cod_judet]
    )

    const uatResult = await pool.query(
      `
      SELECT *
      FROM v_dashboard_uat_52
      WHERE cod_judet = $1
      ORDER BY total_puncte DESC, nume_uat
      `,
      [cod_judet]
    )

    if (summaryResult.rows.length === 0) {
      return res.status(404).json({
        error: 'County not found',
      })
    }

    res.json({
      summary: summaryResult.rows[0],
      routes: routesResult.rows,
      uat: uatResult.rows,
    })
  } catch (error) {
    console.error('GET /api/harta/judete/:cod_judet error:', error)
    res.status(500).json({
      error: 'Failed to fetch county details',
    })
  }
})

app.get('/api/harta/judete/:cod_judet/puncte', async (req, res) => {
  const { cod_judet } = req.params

  try {
    const result = await pool.query(
      `
      SELECT
        punct_id,
        cod_punct,
        cod_ruta,
        nume_ruta,
        tip_indicator,
        tip_drum,
        regim_documente,
        autoritate_politie_cod,
        autoritate_admin_cod,
        status_operational,
        lat,
        long
      FROM v_prioritizare_puncte_roa_52
      WHERE cod_judet = $1
      ORDER BY cod_ruta, cod_punct
      `,
      [cod_judet]
    )

    res.json(result.rows)
  } catch (error) {
    console.error('GET /api/harta/judete/:cod_judet/puncte error:', error)
    res.status(500).json({
      error: 'Failed to fetch county points',
    })
  }
})

app.post('/api/montaj/poze/sync', async (req, res) => {
  const {
    cod_stalp,
    drive_file_id,
    drive_file_url,
    drive_folder_url,
    filename,
    uploaded_at,
    lat_exif,
    long_exif,
  } = req.body || {}

  if (!cod_stalp || !drive_file_url) {
    return res.status(400).json({
      ok: false,
      error: 'cod_stalp and drive_file_url are required',
    })
  }

  try {
    await pool.query(
      `
      INSERT INTO montaj_stalpi_poze (
        cod_stalp,
        drive_file_id,
        drive_file_url,
        drive_folder_url,
        filename,
        uploaded_at,
        lat_exif,
        long_exif
      )
      VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
      ON CONFLICT DO NOTHING
      `,
      [
        cod_stalp,
        drive_file_id || null,
        drive_file_url,
        drive_folder_url || null,
        filename || null,
        uploaded_at || null,
        lat_exif || null,
        long_exif || null,
      ]
    )

    await pool.query(
      `
      UPDATE montaj_stalpi ms
      SET
          nr_poze = src.nr_poze,
          montat = CASE
              WHEN src.nr_poze >= 2 THEN TRUE
              ELSE FALSE
          END,
          status_montaj = CASE
              WHEN src.nr_poze >= 2 THEN 'montat'
              WHEN src.nr_poze = 1 THEN 'in_verificare'
              ELSE 'nemontat'
          END,
          data_confirmare = CASE
              WHEN src.nr_poze >= 2 THEN src.ultima_poza_at
              ELSE ms.data_confirmare
          END,
          drive_folder_url = src.drive_folder_url,
          ultima_poza_url = src.ultima_poza_url,
          ultima_poza_at = src.ultima_poza_at,
          lat_confirmat = COALESCE(src.lat_exif, ms.lat_confirmat),
          long_confirmat = COALESCE(src.long_exif, ms.long_confirmat),
          sursa_confirmare = CASE
              WHEN src.nr_poze >= 2 THEN 'drive_foto'
              ELSE ms.sursa_confirmare
          END,
          updated_at = NOW()
      FROM (
          SELECT
              cod_stalp,
              COUNT(*) AS nr_poze,
              MAX(uploaded_at) AS ultima_poza_at,
              MAX(drive_file_url) AS ultima_poza_url,
              MAX(drive_folder_url) AS drive_folder_url,
              MAX(lat_exif) AS lat_exif,
              MAX(long_exif) AS long_exif
          FROM montaj_stalpi_poze
          WHERE cod_stalp = $1
          GROUP BY cod_stalp
      ) src
      WHERE src.cod_stalp = ms.cod_stalp
      `,
      [cod_stalp]
    )

    const statusResult = await pool.query(
      `
      SELECT
        cod_stalp,
        montat,
        status_montaj,
        nr_poze,
        data_confirmare,
        drive_folder_url,
        ultima_poza_url
      FROM montaj_stalpi
      WHERE cod_stalp = $1
      `,
      [cod_stalp]
    )

    return res.json({
      ok: true,
      cod_stalp,
      stalp: statusResult.rows[0] || null,
    })
  } catch (error) {
    console.error('POST /api/montaj/poze/sync error:', error)
    return res.status(500).json({
      ok: false,
      error: 'Failed to sync photo',
      details: error.message,
    })
  }
})

app.post('/api/montaj/poze/sync-bulk', async (req, res) => {
  const { items } = req.body || {}

  if (!Array.isArray(items)) {
    return res.status(400).json({
      ok: false,
      error: 'items must be an array',
    })
  }

  const client = await pool.connect()

  try {
    const rawCoduri = [...new Set(items.map((x) => x?.cod_stalp).filter(Boolean))]

    const existingResult = await client.query(
      `
      SELECT cod_stalp
      FROM montaj_stalpi
      WHERE cod_stalp = ANY($1)
      `,
      [rawCoduri]
    )

    const existingSet = new Set(existingResult.rows.map((r) => r.cod_stalp))

    const validItems = items.filter(
      (item) => item?.cod_stalp && item?.drive_file_url && existingSet.has(item.cod_stalp)
    )

    const skippedItems = items.filter(
      (item) => !item?.cod_stalp || !item?.drive_file_url || !existingSet.has(item.cod_stalp)
    )

    const skippedCoduri = [...new Set(skippedItems.map((x) => x?.cod_stalp).filter(Boolean))]

    await client.query('BEGIN')

    for (const item of validItems) {
      const {
        cod_stalp,
        drive_file_id,
        drive_file_url,
        drive_folder_url,
        filename,
        uploaded_at,
        lat_exif,
        long_exif,
      } = item || {}

      await client.query(
        `
        INSERT INTO montaj_stalpi_poze (
          cod_stalp,
          drive_file_id,
          drive_file_url,
          drive_folder_url,
          filename,
          uploaded_at,
          lat_exif,
          long_exif
        )
        VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
        ON CONFLICT DO NOTHING
        `,
        [
          cod_stalp,
          drive_file_id || null,
          drive_file_url,
          drive_folder_url || null,
          filename || null,
          uploaded_at || null,
          lat_exif || null,
          long_exif || null,
        ]
      )
    }

    const coduriValide = [...new Set(validItems.map((x) => x.cod_stalp).filter(Boolean))]

    for (const cod_stalp of coduriValide) {
      await client.query(
        `
        UPDATE montaj_stalpi ms
        SET
            nr_poze = src.nr_poze,
            montat = CASE
                WHEN src.nr_poze >= 2 THEN TRUE
                ELSE FALSE
            END,
            status_montaj = CASE
                WHEN src.nr_poze >= 2 THEN 'montat'
                WHEN src.nr_poze = 1 THEN 'in_verificare'
                ELSE 'nemontat'
            END,
            data_confirmare = CASE
                WHEN src.nr_poze >= 2 THEN src.ultima_poza_at
                ELSE ms.data_confirmare
            END,
            drive_folder_url = src.drive_folder_url,
            ultima_poza_url = src.ultima_poza_url,
            ultima_poza_at = src.ultima_poza_at,
            lat_confirmat = COALESCE(src.lat_exif, ms.lat_confirmat),
            long_confirmat = COALESCE(src.long_exif, ms.long_confirmat),
            sursa_confirmare = CASE
                WHEN src.nr_poze >= 2 THEN 'drive_foto'
                ELSE ms.sursa_confirmare
            END,
            updated_at = NOW()
        FROM (
            SELECT
                cod_stalp,
                COUNT(*) AS nr_poze,
                MAX(uploaded_at) AS ultima_poza_at,
                MAX(drive_file_url) AS ultima_poza_url,
                MAX(drive_folder_url) AS drive_folder_url,
                MAX(lat_exif) AS lat_exif,
                MAX(long_exif) AS long_exif
            FROM montaj_stalpi_poze
            WHERE cod_stalp = $1
            GROUP BY cod_stalp
        ) src
        WHERE src.cod_stalp = ms.cod_stalp
        `,
        [cod_stalp]
      )
    }

    await client.query('COMMIT')

    return res.json({
      ok: true,
      total_items: items.length,
      total_valid_items: validItems.length,
      total_skipped_items: skippedItems.length,
      total_stalpi_actualizati: coduriValide.length,
      skipped_cod_stalp: skippedCoduri.slice(0, 50),
    })
  } catch (error) {
    await client.query('ROLLBACK')
    console.error('POST /api/montaj/poze/sync-bulk error:', error)
    return res.status(500).json({
      ok: false,
      error: 'Failed to sync bulk photos',
      details: error.message,
    })
  } finally {
    client.release()
  }
})

app.post('/api/montaj/progres/sync-bulk', async (req, res) => {
  const { items } = req.body || {}

  if (!Array.isArray(items)) {
    return res.status(400).json({
      ok: false,
      error: 'items must be an array',
    })
  }

  function normalizeCodStalpFromSheet(raw) {
    if (!raw) return null

    const value = String(raw).trim().toUpperCase()
    if (!value) return null

    if (value.includes('_')) return value

    const match = value.match(/^(R\d+)(.+)$/i)
    if (!match) return value

    const ruta = match[1].toUpperCase()
    return `${ruta}_${value}`
  }

  function parseDate(value) {
    if (!value) return null

    const s = String(value).trim()
    if (!s) return null

    if (/^\d{4}-\d{2}-\d{2}$/.test(s)) return s

    const m = s.match(/^(\d{1,2})\.(\d{1,2})\.(\d{4})$/)
    if (m) {
      const dd = m[1].padStart(2, '0')
      const mm = m[2].padStart(2, '0')
      const yyyy = m[3]
      return `${yyyy}-${mm}-${dd}`
    }

    return null
  }

  const client = await pool.connect()

  try {
    const normalizedItems = items
      .map((item) => {
        const cod_stalp = normalizeCodStalpFromSheet(item?.cod_stalp)
        const data_confirmare = parseDate(item?.data_confirmare)

        return {
          cod_stalp,
          data_confirmare,
          sursa_raw: item?.cod_stalp || null,
        }
      })
      .filter((x) => x.cod_stalp)

    const coduri = [...new Set(normalizedItems.map((x) => x.cod_stalp))]

    const existingResult = await client.query(
      `
      SELECT cod_stalp
      FROM montaj_stalpi
      WHERE cod_stalp = ANY($1)
      `,
      [coduri]
    )

    const existingSet = new Set(existingResult.rows.map((r) => r.cod_stalp))

    const validItems = normalizedItems.filter((x) => existingSet.has(x.cod_stalp))
    const skippedItems = normalizedItems.filter((x) => !existingSet.has(x.cod_stalp))

    await client.query('BEGIN')

    for (const item of validItems) {
      if (item.data_confirmare) {
        await client.query(
          `
          UPDATE montaj_stalpi
          SET
            montat = TRUE,
            status_montaj = 'montat',
            data_confirmare = $2::date,
            sursa_confirmare = 'sheet_progress',
            updated_at = NOW()
          WHERE cod_stalp = $1
          `,
          [item.cod_stalp, item.data_confirmare]
        )
      } else {
        await client.query(
          `
          UPDATE montaj_stalpi
          SET
            montat = FALSE,
            status_montaj = 'nemontat',
            data_confirmare = NULL,
            sursa_confirmare = CASE
              WHEN sursa_confirmare = 'sheet_progress' THEN 'sheet_progress'
              ELSE sursa_confirmare
            END,
            updated_at = NOW()
          WHERE cod_stalp = $1
          `,
          [item.cod_stalp]
        )
      }
    }

    await client.query('COMMIT')

    return res.json({
      ok: true,
      total_items: items.length,
      total_normalized_items: normalizedItems.length,
      total_valid_items: validItems.length,
      total_skipped_items: skippedItems.length,
      skipped_cod_stalp: skippedItems.slice(0, 50).map((x) => x.sursa_raw || x.cod_stalp),
    })
  } catch (error) {
    await client.query('ROLLBACK')
    console.error('POST /api/montaj/progres/sync-bulk error:', error)
    return res.status(500).json({
      ok: false,
      error: 'Failed to sync sheet progress',
      details: error.message,
    })
  } finally {
    client.release()
  }
})

app.get('/api/montaj/judete', async (req, res) => {
  try {
    const result = await pool.query(`
      SELECT *
      FROM v_financiar_stalpi_judet
      ORDER BY cod_judet
    `)

    res.json(result.rows)
  } catch (error) {
    console.error('GET /api/montaj/judete error:', error)
    res.status(500).json({
      error: 'Failed to fetch montaj dashboard',
    })
  }
})

app.get('/api/montaj/stalpi/:cod_judet', async (req, res) => {
  const { cod_judet } = req.params

  try {
    const result = await pool.query(
      `
      SELECT *
      FROM v_stalpi_operational
      WHERE cod_judet = $1
      ORDER BY montat DESC, cod_stalp
      `,
      [cod_judet]
    )

    res.json(result.rows)
  } catch (error) {
    console.error('GET /api/montaj/stalpi/:cod_judet error:', error)
    res.status(500).json({
      error: 'Failed to fetch stalpi',
    })
  }
})

app.get('/api/montaj/judete-summary', async (req, res) => {
  try {
    const result = await pool.query(`
      SELECT
        vmj.cod_judet,
        vmj.nume_judet,
        vmj.stalpi_eligibili,
        vmj.stalpi_montati,
        vmj.stalpi_ramasi,
        vmj.stalpi_in_verificare,
        vmj.procent_montat,
        vfj.venit_total,
        vfj.marja_estimativa,
        vfj.marja_la_zi
      FROM v_montaj_judet_executiv vmj
      LEFT JOIN v_financiar_stalpi_judet vfj
        ON vfj.cod_judet = vmj.cod_judet
      ORDER BY vmj.cod_judet
    `)

    res.json(result.rows)
  } catch (error) {
    console.error('GET /api/montaj/judete-summary error:', error)
    res.status(500).json({
      error: 'Failed to fetch montaj county summary',
    })
  }
})

app.listen(port, '0.0.0.0', () => {
  console.log(`ROA API running on port ${port}`)
})