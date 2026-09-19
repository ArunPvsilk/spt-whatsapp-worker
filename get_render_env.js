const fs = require('fs');
const path = require('path');

const keyPath = path.join(__dirname, 'service-account-key.json');

if (!fs.existsSync(keyPath)) {
  console.error('Error: service-account-key.json not found in server/whatsapp-worker/');
  process.exit(1);
}

try {
  const jsonContent = fs.readFileSync(keyPath, 'utf8');
  // Minify JSON to single line
  const minified = JSON.stringify(JSON.parse(jsonContent));
  
  console.log('\n=============================================================');
  console.log('  RENDER.COM ENVIRONMENT VARIABLE CONFIGURATION');
  console.log('=============================================================\n');
  console.log('In Render Dashboard -> Environment -> Environment Variables:\n');
  console.log('KEY:');
  console.log('FIREBASE_SERVICE_ACCOUNT\n');
  console.log('VALUE (Copy the full line below):');
  console.log(minified);
  console.log('\n=============================================================\n');
} catch (e) {
  console.error('Failed to parse service-account-key.json:', e.message);
}
