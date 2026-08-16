import {execFileSync} from 'node:child_process';

const greeting = execFileSync(
  'tickr-ctx',
  ['get', 'greeting', '--signal', '--default', 'Hello from Tickr'],
  {encoding: 'utf8'},
).trim();
console.log(`javascript: ${greeting}`);
