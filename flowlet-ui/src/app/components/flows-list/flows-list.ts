import { Component, OnInit } from '@angular/core';
import { CommonModule } from '@angular/common';
import { RouterModule } from '@angular/router';
import { NgbModal } from '@ng-bootstrap/ng-bootstrap';
import { FlowletApi, FlowListItem } from '../../services/flowlet-api';
import { RunFlowModal } from '../run-flow-modal/run-flow-modal';

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

  constructor(
    private flowletApi: FlowletApi,
    private modalService: NgbModal
  ) {}

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
    const modalRef = this.modalService.open(RunFlowModal, {
      size: 'lg',
      centered: true
    });
    modalRef.componentInstance.flowName = flowName;

    modalRef.result.then(
      (kwargs) => {
        // User clicked "Run Flow"
        this.flowletApi.runFlow(flowName, { kwargs }).subscribe({
          next: (run) => {
            alert(`Flow "${flowName}" started with run ID: ${run.flow_id}`);
            this.loadFlows(); // Refresh the list
          },
          error: (err) => {
            alert('Failed to start flow: ' + err.message);
          }
        });
      },
      (reason) => {
        // User dismissed the modal (clicked cancel or close)
        console.log('Modal dismissed:', reason);
      }
    );
  }
}
