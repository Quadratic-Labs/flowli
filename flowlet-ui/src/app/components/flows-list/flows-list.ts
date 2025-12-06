import { Component, OnInit } from '@angular/core';
import { CommonModule } from '@angular/common';
import { RouterModule } from '@angular/router';
import { MatTableModule } from '@angular/material/table';
import { MatButtonModule } from '@angular/material/button';
import { MatProgressSpinnerModule } from '@angular/material/progress-spinner';
import { MatChipsModule } from '@angular/material/chips';
import { MatIconModule } from '@angular/material/icon';
import { MatSnackBar, MatSnackBarModule } from '@angular/material/snack-bar';
import { FlowletApi, FlowListItem } from '../../services/flowlet-api';

@Component({
  selector: 'app-flows-list',
  imports: [CommonModule, RouterModule, MatTableModule, MatButtonModule, MatProgressSpinnerModule, MatChipsModule, MatIconModule, MatSnackBarModule],
  templateUrl: './flows-list.html',
  styleUrl: './flows-list.css',
})
export class FlowsList implements OnInit {
  flows: FlowListItem[] = [];
  loading = true;
  error: string | null = null;
  displayedColumns: string[] = ['name', 'status', 'finished', 'duration', 'description', 'actions'];

  constructor(private flowletApi: FlowletApi, private snackBar: MatSnackBar) {}

  ngOnInit(): void {
    this.loadFlows();
  }

  loadFlows(): void {
    this.loading = true;
    this.error = null;
    this.flowletApi.getFlows().subscribe({
      next: (flows) => {
        this.flows = flows;
        this.loading = false;
      },
      error: (err) => {
        this.error = 'Failed to load flows: ' + err.message;
        this.loading = false;
      }
    });
  }

  runFlow(flowName: string): void {
    this.flowletApi.runFlow(flowName).subscribe({
      next: () => {
        this.snackBar.open(`Flow "${flowName}" started successfully`, 'Close', { duration: 3000 });
        this.loadFlows();
      },
      error: (err) => {
        this.snackBar.open('Failed to start flow: ' + err.message, 'Close', { duration: 5000 });
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
