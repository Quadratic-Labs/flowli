import { Routes } from '@angular/router';
import { FlowsList } from './components/flows-list/flows-list';
import { RunsList } from './components/runs-list/runs-list';
import { RunDetail } from './components/run-detail/run-detail';

export const routes: Routes = [
  { path: '', component: FlowsList },
  { path: 'runs', component: RunsList },
  { path: 'runs/:id', component: RunDetail },
  { path: '**', redirectTo: '' }
];
