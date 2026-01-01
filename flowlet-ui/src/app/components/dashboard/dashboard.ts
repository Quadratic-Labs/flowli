import { Component, OnInit } from '@angular/core';
import { CommonModule } from '@angular/common';
import { MatTableModule } from '@angular/material/table';
import { MatButtonModule } from '@angular/material/button';
import { MatCardModule } from '@angular/material/card';
import { MatIconModule } from '@angular/material/icon';
import { MatProgressSpinnerModule } from '@angular/material/progress-spinner';
import { MatChipsModule } from '@angular/material/chips';
import { Router } from '@angular/router';
import { FlowletApi, DashboardMetrics } from '../../services/flowlet-api';

@Component({
  selector: 'app-dashboard',
  standalone: true,
  imports: [
    CommonModule,
    MatTableModule,
    MatButtonModule,
    MatCardModule,
    MatIconModule,
    MatProgressSpinnerModule,
    MatChipsModule,
  ],
  templateUrl: './dashboard.html',
  styleUrls: ['./dashboard.css'],
})
export class Dashboard implements OnInit {
  loading = true;
  error: string | null = null;
  metrics: DashboardMetrics | null = null;
  failuresDisplayedColumns: string[] = ['name', 'status', 'duration', 'started', 'actions'];

  constructor(
    private api: FlowletApi,
    private router: Router
  ) {}

  ngOnInit() {
    this.loadDashboard();
  }

  async loadDashboard() {
    this.loading = true;
    this.error = null;

    try {
      const metrics = await this.api.getDashboardMetrics();
      this.metrics = metrics;
    } catch (err) {
      this.error = err instanceof Error ? err.message : 'Failed to load dashboard';
      console.error('Dashboard error:', err);
    } finally {
      this.loading = false;
    }
  }

  getAllFailures(): any[] {
    if (!this.metrics) return [];
    return [...this.metrics.failedRuns, ...this.metrics.longRunningFlows];
  }

  getStatusColor(status: string): string {
    const statusLower = status.toLowerCase();
    if (statusLower === 'success' || statusLower === 'completed') return '#4caf50';
    if (statusLower === 'failed' || statusLower === 'error' || statusLower === 'critical') return '#f44336';
    if (statusLower === 'running' || statusLower === 'warning' || statusLower === 'pending') return '#ff9800';
    return '#9e9e9e';
  }

  getHealthIcon(): string {
    if (!this.metrics) return 'help';
    switch (this.metrics.healthStatus) {
      case 'green': return 'check_circle';
      case 'orange': return 'warning';
      case 'red': return 'error';
      default: return 'help';
    }
  }

  getHealthColor(): string {
    if (!this.metrics) return '#9e9e9e';
    switch (this.metrics.healthStatus) {
      case 'green': return '#4caf50';
      case 'orange': return '#ff9800';
      case 'red': return '#f44336';
      default: return '#9e9e9e';
    }
  }

  getHealthText(): string {
    if (!this.metrics) return 'Unknown';
    switch (this.metrics.healthStatus) {
      case 'green': return 'Healthy';
      case 'orange': return 'Warning';
      case 'red': return 'Critical';
      default: return 'Unknown';
    }
  }

  viewRunDetail(runId: string) {
    this.router.navigate(['/runs', runId]);
  }

  formatTimestamp(timestamp: string): string {
    const date = new Date(timestamp);
    return date.toLocaleString();
  }

  calculateTimeAgo(timestamp: string): string {
    const now = new Date();
    const then = new Date(timestamp);
    const diffMs = now.getTime() - then.getTime();
    const diffMins = Math.floor(diffMs / 60000);
    const diffHours = Math.floor(diffMins / 60);
    const diffDays = Math.floor(diffHours / 24);

    if (diffDays > 0) return `${diffDays}d ago`;
    if (diffHours > 0) return `${diffHours}h ago`;
    if (diffMins > 0) return `${diffMins}m ago`;
    return 'just now';
  }
}
