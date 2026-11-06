/**
 * Main third-party runtime components (console bundle + hub image, `console/package.json` and
 * `deploy/image/requirements.txt`). Each keeps its own licence; full texts ship with the packages themselves.
 */
export interface ThirdParty {
  name: string;
  part: 'console' | 'hub';
  licence: string;
  url: string;
}

export const THIRD_PARTY: readonly ThirdParty[] = [
  { name: 'React / React DOM', part: 'console', licence: 'MIT', url: 'https://github.com/facebook/react' },
  { name: 'React Router', part: 'console', licence: 'MIT', url: 'https://github.com/remix-run/react-router' },
  { name: 'Python', part: 'hub', licence: 'PSF-2.0', url: 'https://www.python.org/' },
  { name: 'FastAPI', part: 'hub', licence: 'MIT', url: 'https://github.com/fastapi/fastapi' },
  { name: 'Uvicorn', part: 'hub', licence: 'BSD-3-Clause', url: 'https://github.com/encode/uvicorn' },
  { name: 'Pydantic', part: 'hub', licence: 'MIT', url: 'https://github.com/pydantic/pydantic' },
  { name: 'SQLAlchemy', part: 'hub', licence: 'MIT', url: 'https://www.sqlalchemy.org/' },
  { name: 'psycopg 3', part: 'hub', licence: 'LGPL-3.0', url: 'https://www.psycopg.org/' },
  { name: 'psycopg2-binary', part: 'hub', licence: 'LGPL-3.0 (with exceptions)', url: 'https://www.psycopg.org/' },
  { name: 'PyYAML', part: 'hub', licence: 'MIT', url: 'https://github.com/yaml/pyyaml' },
  { name: 'PyJWT', part: 'hub', licence: 'MIT', url: 'https://github.com/jpadilla/pyjwt' },
  { name: 'zstandard (python-zstandard)', part: 'hub', licence: 'BSD-3-Clause', url: 'https://github.com/indygreg/python-zstandard' },
  { name: 'cffi', part: 'hub', licence: 'MIT', url: 'https://github.com/python-cffi/cffi' },
  { name: 'HTTPX', part: 'hub', licence: 'BSD-3-Clause', url: 'https://github.com/encode/httpx' },
  { name: 'Apache Arrow (pyarrow)', part: 'hub', licence: 'Apache-2.0', url: 'https://arrow.apache.org/' },
  { name: 'cryptography', part: 'hub', licence: 'Apache-2.0 OR BSD-3-Clause', url: 'https://github.com/pyca/cryptography' },
  { name: 'Snowflake Connector for Python', part: 'hub', licence: 'Apache-2.0', url: 'https://github.com/snowflakedb/snowflake-connector-python' },
];
