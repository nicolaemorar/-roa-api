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
    await client.query('BEGIN')

    for (const item of items) {
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

      if (!cod_stalp || !drive_file_url) continue

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

    const coduri = [...new Set(items.map((x) => x.cod_stalp).filter(Boolean))]

    for (const cod_stalp of coduri) {
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
      total_stalpi_actualizati: coduri.length,
    })
  } catch (error) {
    await client.query('ROLLBACK')
    console.error('POST /api/montaj/poze/sync-bulk error:', error)
    return res.status(500).json({
      ok: false,
      error: 'Failed to sync bulk photos',
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