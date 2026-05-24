const http = require('http');
const url  = require('url');

const PORT = process.env.PORT || 3000;

// ── SSE CLIENTS ──
// Holds all connected dashboard browser tabs
let clients = [];

function addClient(res) {
  res.writeHead(200, {
    'Content-Type':  'text/event-stream',
    'Cache-Control': 'no-cache',
    'Connection':    'keep-alive',
    'Access-Control-Allow-Origin': '*',
  });
  res.write('data: {"type":"connected"}\n\n');
  clients.push(res);
  return res;
}

function removeClient(res) {
  clients = clients.filter(c => c !== res);
}

function broadcast(data) {
  const payload = `data: ${JSON.stringify(data)}\n\n`;
  clients.forEach(c => {
    try { c.write(payload); } catch {}
  });
}

// Keep SSE connections alive with a heartbeat every 20s
setInterval(() => {
  clients.forEach(c => {
    try { c.write(': heartbeat\n\n'); } catch {}
  });
}, 20000);

// ── REQUEST HANDLER ──
const server = http.createServer((req, res) => {
  const parsed   = url.parse(req.url, true);
  const pathname = parsed.pathname;
  const method   = req.method;

  // CORS — allow dashboard HTML file opened locally
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type');

  if (method === 'OPTIONS') {
    res.writeHead(204);
    res.end();
    return;
  }

  // ── GET /events — dashboard connects here for live signals ──
  if (method === 'GET' && pathname === '/events') {
    const client = addClient(res);
    req.on('close', () => removeClient(client));
    return;
  }

  // ── GET /health — Railway and browser health check ──
  if (method === 'GET' && pathname === '/health') {
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ status: 'ok', clients: clients.length }));
    return;
  }

  // ── POST /webhook — TradingView sends alerts here ──
  if (method === 'POST' && pathname === '/webhook') {
    let body = '';

    req.on('data', chunk => { body += chunk; });

    req.on('end', () => {
      try {
        const data = JSON.parse(body);

        // Validate required fields
        if (!data.pair || !data.signal || !data.strat) {
          res.writeHead(400, { 'Content-Type': 'application/json' });
          res.end(JSON.stringify({ error: 'Missing required fields: pair, signal, strat' }));
          return;
        }

        // Normalise signal object
        const signal = {
          type:        'signal',
          pair:        data.pair,
          tf:          data.tf          || 'M15',
          signal:      data.signal,       // 'bull' | 'bear'
          strat:       data.strat,        // 'IB' | 'TRAP' | 'MIB'
          score:       parseInt(data.score) || 0,
          confluence:  data.confluence   || '',
          tfs:         data.tfs          || '',  // for Multi IB e.g. "M30+H1"
          ts:          Date.now(),
        };

        console.log(`[${new Date().toISOString()}] Signal received:`, JSON.stringify(signal));

        // Push to all connected dashboards
        broadcast(signal);

        res.writeHead(200, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ ok: true, clients: clients.length }));

      } catch (e) {
        console.error('Webhook parse error:', e.message, '| Body:', body);
        res.writeHead(400, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ error: 'Invalid JSON' }));
      }
    });

    return;
  }

  // ── 404 for everything else ──
  res.writeHead(404, { 'Content-Type': 'application/json' });
  res.end(JSON.stringify({ error: 'Not found' }));
});

server.listen(PORT, () => {
  console.log(`Signal Dashboard server running on port ${PORT}`);
  console.log(`Endpoints:`);
  console.log(`  GET  /health   — health check`);
  console.log(`  GET  /events   — SSE stream for dashboard`);
  console.log(`  POST /webhook  — TradingView webhook receiver`);
});
