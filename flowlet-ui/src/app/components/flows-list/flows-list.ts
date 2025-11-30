import { Component, OnInit } from '@angular/core';
import { CommonModule } from '@angular/common';
import { RouterModule } from '@angular/router';
import { FlowletApi, FlowListItem } from '../../services/flowlet-api';

@Component({
  selector: 'app-flows-list',
  imports: [CommonModule, RouterModule],
  templateUrl: './flows-list.html',
  styleUrl: './flows-list.css',
})
export class FlowsList implements OnInit {
  flows: FlowListItem[] = [];
  loading = true;
  error: string | null = null;

  constructor(private flowletApi: FlowletApi) {}

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
        alert(`Flow "${flowName}" started successfully`);
        this.loadFlows();
      },
      error: (err) => {
        alert('Failed to start flow: ' + err.message);
      }
    });
  }
}
