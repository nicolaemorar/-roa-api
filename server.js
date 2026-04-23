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