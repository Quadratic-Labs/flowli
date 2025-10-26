import { Component, OnInit } from '@angular/core';
import { CommonModule } from '@angular/common';
import { RouterModule } from '@angular/router';
import { FlowletApi, FlowRunInfo } from '../../services/flowlet-api';

@Component({
  selector: 'app-runs-list',
  imports: [CommonModule, RouterModule],
  templateUrl: './runs-list.html',
  styleUrl: './runs-list.css',
})
export class RunsList implements OnInit {
  runs: FlowRunInfo[] = [];
  loading = true;
  error: string | null = null;
  offset = 0;
  limit = 50;

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
}
