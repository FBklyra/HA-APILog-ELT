import { BrowserRouter, Route, Routes } from 'react-router';
import { AuthProvider, useAuth } from './auth/AuthProvider';
import { Guard, Layout } from './components/Layout';
import { Loading } from './components/ui';
import { I18nProvider, useI18n } from './i18n';
import { Certification } from './screens/Certification';
import { SchemaImport, Revisions, SchemaChanges } from './screens/Config';
import { AiAssistant } from './screens/AiAssistant';
import { DenyListScreen } from './screens/DenyList';
import { IncidentDetailScreen, Incidents } from './screens/Incidents';
import { ImportWizard } from './screens/ImportWizard';
import { Overview } from './screens/Overview';
import { ParallelTestWizard } from './screens/ParallelTestWizard';
import { Pipelines } from './screens/Pipelines';
import { Agent, Audit, Hub, RetentionScreen, Users } from './screens/Platform';
import { Recon } from './screens/Recon';
import { Setup } from './screens/Setup';
import { TableDetail } from './screens/TableDetail';
import { Tables } from './screens/Tables';
import { Topology } from './screens/Topology';
import { About } from './screens/About';
import { Lab } from './screens/Lab';

function Routed() {
  const { me, error, dev, signIn } = useAuth();
  const { t } = useI18n();
  if (!me) {
    return (
      <Layout>
        {error ? <Loading error={error} /> : dev ? <Loading /> : (
          <div className="signin"><button className="primary" onClick={signIn}>{t('auth.signIn')}</button></div>
        )}
      </Layout>
    );
  }
  return (
    <Layout>
      <Routes>
        <Route path="/" element={<Guard p="view.dashboard"><Overview /></Guard>} />
        <Route path="/tables" element={<Guard p="view.tables"><Tables /></Guard>} />
        <Route path="/tables/:id" element={<Guard p="view.tables"><TableDetail /></Guard>} />
        <Route path="/import" element={<Guard p="import.run"><SchemaImport /></Guard>} />
        <Route path="/import/new" element={<Guard p="import.run"><ImportWizard /></Guard>} />
        <Route path="/wizard" element={<Guard p="table.configure"><ParallelTestWizard /></Guard>} />
        <Route path="/pipelines" element={<Guard p="view.dashboard"><Pipelines /></Guard>} />
        <Route path="/guard" element={<Guard p="users.manage"><DenyListScreen /></Guard>} />
        <Route path="/revisions" element={<Guard p="table.configure"><Revisions /></Guard>} />
        <Route path="/schema-changes" element={<Guard p="schema.decide"><SchemaChanges /></Guard>} />
        <Route path="/recon" element={<Guard p="view.recon"><Recon /></Guard>} />
        <Route path="/certification" element={<Guard p="view.certification"><Certification /></Guard>} />
        <Route path="/agent" element={<Guard p="agent.view"><Agent /></Guard>} />
        <Route path="/hub" element={<Guard p="hub.view"><Hub /></Guard>} />
        <Route path="/topology" element={<Guard p="hub.view"><Topology /></Guard>} />
        <Route path="/retention" element={<Guard p="retention.edit"><RetentionScreen /></Guard>} />
        <Route path="/users" element={<Guard p="users.manage"><Users /></Guard>} />
        <Route path="/audit" element={<Guard p="audit.read"><Audit /></Guard>} />
        <Route path="/setup" element={<Guard p="users.manage"><Setup /></Guard>} />
        <Route path="/incidents" element={<Guard p="view.incidents"><Incidents /></Guard>} />
        <Route path="/incidents/:id" element={<Guard p="view.incidents"><IncidentDetailScreen /></Guard>} />
        <Route path="/ai-assistant" element={<Guard p="llm.configure"><AiAssistant /></Guard>} />
        <Route path="/lab" element={<Guard p="users.manage"><Lab /></Guard>} />
        <Route path="/about" element={<Guard p="view.dashboard"><About /></Guard>} />
        <Route path="*" element={<p>{t('common.notFound')}</p>} />
      </Routes>
    </Layout>
  );
}

export function App() {
  return (
    <I18nProvider>
      <AuthProvider>
        <BrowserRouter>
          <Routed />
        </BrowserRouter>
      </AuthProvider>
    </I18nProvider>
  );
}
