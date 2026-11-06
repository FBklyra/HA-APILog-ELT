// SPDX-License-Identifier: Apache-2.0
import { useEffect, useState } from 'react';
import { version as consoleVersion } from '../../package.json';
import { call } from '../api/product';
import { useAuth } from '../auth/AuthProvider';
import { Card, Page, Table } from '../components/ui';
import { useI18n } from '../i18n';
import { THIRD_PARTY } from '../lib/thirdParty';

export const PRODUCT_NAME = 'Universal HA API Log ELT (ALELT)';

/** Engine → vendor public interface used for change capture (docs/PUBLIC-APIS.md). Technical names, not translated. */
export const PUBLIC_APIS: readonly { engine: string; api: string }[] = [
  { engine: 'IBM Db2 LUW', api: 'db2ReadLog API' },
  { engine: 'PostgreSQL', api: 'Logical decoding / pgoutput (streaming replication protocol)' },
  { engine: 'MySQL / MariaDB', api: 'Replication protocol (binlog events)' },
  { engine: 'Oracle Database', api: 'LogMiner (DBMS_LOGMNR, V$LOGMNR_CONTENTS)' },
  { engine: 'SQL Server', api: 'Change data capture table functions (cdc.fn_cdc_get_all_changes_*)' },
];

interface HubVersion { version?: string; revision?: string; build_date?: string }

const Files = ({ names }: { names: string[] }) => (
  <p className="hint">{names.map((n, i) => <span key={n}>{i > 0 ? ' · ' : ''}<code>{n}</code></span>)}</p>
);

/** About & Legal: licence / no-liability / public-information statements first, then version and third-party notices. */
export function About() {
  const { t } = useI18n();
  const { authHeaders } = useAuth();
  const [hub, setHub] = useState<HubVersion | null>(null);
  useEffect(() => {
    let live = true;
    call<HubVersion>(authHeaders, 'GET', '/version').then((v) => live && setHub(v), () => live && setHub(null));
    return () => { live = false; };
  }, [authHeaders]);

  return (
    <Page title={t('nav.about')}>
      <div className="card" role="region" aria-label={t('about.legal.title')} data-testid="about-legal"
        style={{ border: '2px solid var(--accent, currentColor)' }}>
        <h2>{t('about.legal.title')}</h2>
        <ol>
          <li data-testid="about-licence">
            <strong>{t('about.licence.title')}</strong>
            <p>{t('about.licence.body')}</p>
            <p><a href="https://www.apache.org/licenses/LICENSE-2.0" target="_blank" rel="noreferrer noopener">{t('about.licence.link')}</a></p>
            <Files names={['LICENSE', 'NOTICE']} />
          </li>
          <li data-testid="about-disclaimer">
            <strong>{t('about.disclaimer.title')}</strong>
            <p>{t('about.disclaimer.asIs')} {t('about.disclaimer.liability')}</p>
            <p>{t('about.disclaimer.yours')}</p>
            <Files names={['DISCLAIMER.md']} />
          </li>
          <li data-testid="about-public">
            <strong>{t('about.publicApi.title')}</strong>
            <p>{t('about.publicApi.intro')}</p>
            <Table head={[t('about.publicApi.engine'), t('about.publicApi.api')]}>
              {PUBLIC_APIS.map((r) => <tr key={r.engine}><td>{r.engine}</td><td>{r.api}</td></tr>)}
            </Table>
            <p>{t('about.publicApi.destinations')}</p>
            <p>{t('about.publicApi.sourcesOnly')}</p>
            <p>{t('about.publicApi.never1')} {t('about.publicApi.never2')}</p>
            <p>{t('about.disclaimer.licences')}</p>
            <p>{t('about.disclaimer.affiliation')}</p>
            <Files names={['docs/PUBLIC-APIS.md', 'docs/legal/PROVENANCE.md']} />
          </li>
        </ol>
      </div>

      <Card title={PRODUCT_NAME}>
        <p>{t('about.tagline')}</p>
        <dl className="kv" data-testid="about-version">
          <dt>{t('about.version')}</dt><dd><code>{hub?.version ?? consoleVersion}</code></dd>
          {hub?.revision ? <><dt>{t('about.revision')}</dt><dd><code>{hub.revision}</code></dd></> : null}
          {hub?.build_date ? <><dt>{t('about.buildDate')}</dt><dd><code>{hub.build_date}</code></dd></> : null}
          <dt>{t('about.consoleVersion')}</dt><dd><code>{consoleVersion}</code></dd>
        </dl>
        {!hub && <p className="hint">{t('about.versionOffline')}</p>}
      </Card>

      <Card title={t('about.thirdParty.title')}>
        <p>{t('about.thirdParty.intro', { file: 'NOTICE' })}</p>
        <Table head={[t('about.thirdParty.component'), t('about.thirdParty.part'), t('about.thirdParty.licence')]}>
          {THIRD_PARTY.map((d) => (
            <tr key={d.name}>
              <td><a href={d.url} target="_blank" rel="noreferrer noopener">{d.name}</a></td>
              <td>{d.part}</td><td>{d.licence}</td>
            </tr>
          ))}
        </Table>
      </Card>
    </Page>
  );
}
