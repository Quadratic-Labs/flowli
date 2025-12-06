import { Component, OnInit } from '@angular/core';
import { CommonModule } from '@angular/common';
import { RouterModule } from '@angular/router';
import { MatTableModule } from '@angular/material/table';
import { MatButtonModule } from '@angular/material/button';
import { MatProgressSpinnerModule } from '@angular/material/progress-spinner';
import { MatChipsModule } from '@angular/material/chips';
import { MatIconModule } from '@angular/material/icon';
import { FlowletApi, FlowRunInfo } from '../../services/flowlet-api';

@Component({
  selector: 'app-runs-list',
  imports: [CommonModule, RouterModule, MatTableModule, MatButtonModule, MatProgressSpinnerModule, MatChipsModule, MatIconModule],
  templateUrl: './runs-list.html',
  styleUrl: './runs-list.css',
})
export class RunsList implements OnInit {
  runs: FlowRunInfo[] = [];
  loading = true;
  error: string | null = null;
  offset = 0;
  limit = 50;
  displayedColumns: string[] = ['name', 'status', 'started', 'finished', 'actions'];

  constructor(private flowletApi: FlowletApi) {}

  ngOnInit(): void {
    this.loadRuns();
  }

  loadRuns(): void {
    this.loading = true;
    this.error = null;
    this.flowletApi.getRuns(this.offset, this.limit).subscribe({
      next: (runs) => {
        this.runs = runs;
        this.loading = false;
      },
      error: (err) => {
        this.error = 'Failed to load runs: ' + err.message;
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
