import { Component, OnInit } from '@angular/core';
import { CommonModule } from '@angular/common';
import { ActivatedRoute, RouterModule } from '@angular/router';
import { MatCardModule } from '@angular/material/card';
import { MatButtonModule } from '@angular/material/button';
import { MatProgressSpinnerModule } from '@angular/material/progress-spinner';
import { MatTableModule } from '@angular/material/table';
import { MatChipsModule } from '@angular/material/chips';
import { MatIconModule } from '@angular/material/icon';
import { FlowletApi, RunDetailInfo } from '../../services/flowlet-api';

@Component({
  selector: 'app-run-detail',
  imports: [CommonModule, RouterModule, MatCardModule, MatButtonModule, MatProgressSpinnerModule, MatTableModule, MatChipsModule, MatIconModule],
  templateUrl: './run-detail.html',
  styleUrl: './run-detail.css',
})
export class RunDetail implements OnInit {
  runId: string = '';
  run: RunDetailInfo | null = null;
  loading = true;
  error: string | null = null;
  logsDisplayedColumns: string[] = ['timestamp', 'status', 'log'];
  childrenDisplayedColumns: string[] = ['name', 'runId', 'type', 'actions'];

  constructor(
    private route: ActivatedRoute,
    private flowletApi: FlowletApi
  ) {}

  ngOnInit(): void {
    this.route.params.subscribe(params => {
      this.runId = params['id'];
      this.loadRun();
    });
  }

  loadRun(): void {
    this.loading = true;
    this.error = null;
    this.flowletApi.getRun(this.runId).subscribe({
      next: (run) => {
        this.run = run;
        this.loading = false;
      },
      error: (err) => {
        this.error = 'Failed to load run: ' + err.message;
        this.loading = false;
      }
    });
  }

  getStatusColor(status: string): 'primary' | 'accent' | 'warn' | undefined {
    switch(status) {
      case 'completed': return 'primary';
      case 'failed': return 'warn';
      case 'running': return 'accent';
      default: return undefined;
    }
  }
}
