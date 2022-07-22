module.exports = {
  timeout: '180s',
  files: ['src/__tests__/*.test.ts'],
  extensions: ['ts', 'js'],
  require: ['ts-node/register', 'tsconfig-paths/register'],
}
