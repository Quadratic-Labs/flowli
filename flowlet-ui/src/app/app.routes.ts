import { Routes } from '@angular/router';
import { Dashboard } from './components/dashboard/dashboard';
import { FlowsList } from './components/flows-list/flows-list';
import { FlowRun } from './components/flow-run/flow-run';
import { RunsList } from './components/runs-list/runs-list';
import { RunDetail } from './components/run-detail/run-detail';

export const routes: Routes = [
  { path: '', component: Dashboard },
  { path: 'flows', component: FlowsList },
  { path: 'flows/:name/run', component: FlowRun },
  { path: 'runs', component: RunsList },
  { path: 'runs/:id', component: RunDetail },
  { path: '**', redirectTo: '' }
];
