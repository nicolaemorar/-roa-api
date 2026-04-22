require('dotenv').config();

const express = require('express');
const cors = require('cors');
const { Pool } = require('pg');

const app = express();
const port = process.env.PORT || 3001;

app.use(cors());
app.use(express.json());

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: {
    rejectUnauthorized: false,
  },
});

app.get('/api/health', async (req, res) => {
  try {
    const result = await pool.query('SELECT NOW() AS server_time');
    res.json({
      ok: true,
      server_time: result.rows[0].server_time,
    });
  } catch (error) {
    console.error('Health check error:', error);
    res.status(500).json({
      ok: false,
      error: 'Database connection failed',
    });
  }
});

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
    `);

    res.json(result.rows);
  } catch (error) {
    console.error('GET /api/harta/judete error:', error);
    res.status(500).json({
      error: 'Failed to fetch county dashboard',
    });
  }
});

app.get('/api/harta/judete/:cod_judet', async (req, res) => {
  const { cod_judet } = req.params;

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
    );

    const routesResult = await pool.query(
      `
      SELECT *
      FROM v_dashboard_ruta_judet_52
      WHERE cod_judet = $1
      ORDER BY total_puncte DESC, cod_ruta
      `,
      [cod_judet]
    );

    const uatResult = await pool.query(
      `
      SELECT *
      FROM v_dashboard_uat_52
      WHERE cod_judet = $1
      ORDER BY total_puncte DESC, nume_uat
      `,
      [cod_judet]
    );

    if (summaryResult.rows.length === 0) {
      return res.status(404).json({
        error: 'County not found',
      });
    }

    res.json({
      summary: summaryResult.rows[0],
      routes: routesResult.rows,
      uat: uatResult.rows,
    });
  } catch (error) {
    console.error('GET /api/harta/judete/:cod_judet error:', error);
    res.status(500).json({
      error: 'Failed to fetch county details',
    });
  }
});

app.get('/api/harta/judete/:cod_judet/puncte', async (req, res) => {
  const { cod_judet } = req.params;

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
    );

    res.json(result.rows);
  } catch (error) {
    console.error('GET /api/harta/judete/:cod_judet/puncte error:', error);
    res.status(500).json({
      error: 'Failed to fetch county points',
    });
  }
});

app.listen(port, '0.0.0.0', () => {
  console.log(`ROA API running on port ${port}`);
});