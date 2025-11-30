import { Component, OnInit } from '@angular/core';
import { CommonModule } from '@angular/common';
import { ActivatedRoute, RouterModule } from '@angular/router';
import { FlowletApi, RunDetailInfo } from '../../services/flowlet-api';

@Component({
  selector: 'app-run-detail',
  imports: [CommonModule, RouterModule],
  templateUrl: './run-detail.html',
  styleUrl: './run-detail.css',
})
export class RunDetail implements OnInit {
  runId: string = '';
  run: RunDetailInfo | null = null;
  loading = true;
  error: string | null = null;

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
}
