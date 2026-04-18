import { execSync } from 'child_process';

try {
    const ver = execSync('java -version 2>&1').toString().split('\n')[0];
    console.log('[closure-compiler] Java available:', ver);
} catch (e) {
    console.warn('[closure-compiler] Java not found, falling back to terser');
}
