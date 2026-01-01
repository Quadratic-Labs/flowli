import { Component, OnInit } from '@angular/core';
import { CommonModule } from '@angular/common';
import { ActivatedRoute, RouterModule } from '@angular/router';
import { MatCardModule } from '@angular/material/card';
import { MatButtonModule } from '@angular/material/button';
import { MatProgressSpinnerModule } from '@angular/material/progress-spinner';
import { MatTableModule } from '@angular/material/table';
import { MatChipsModule } from '@angular/material/chips';
import { MatIconModule } from '@angular/material/icon';
import { FlowletApi, RunDTO, SpanLogDTO } from '../../services/flowlet-api';
import { BreadcrumbService } from '../../services/breadcrumb.service';
import { RunFlamegraph } from '../run-flamegraph/run-flamegraph';
import { forkJoin } from 'rxjs';

// Interface matching the template's expectations (legacy structure)
interface RunDetailDisplay {
  run: {
    name: string;
    run_id: string;
    run_type: string;
  };
  logs: {
    timestamp: string;
    status: string;
    log: string;
  }[];
  parent: {
    name: string;
    run_id: string;
  } | null;
  children: RunDetailDisplay[];
}

@Component({
  selector: 'app-run-detail',
  imports: [CommonModule, RouterModule, MatCardModule, MatButtonModule, MatProgressSpinnerModule, MatTableModule, MatChipsModule, MatIconModule, RunFlamegraph],
  templateUrl: './run-detail.html',
  styleUrl: './run-detail.css',
})
export class RunDetail implements OnInit {
  runId: string = '';
  run: RunDetailDisplay | null = null;
  runDTO: RunDTO | null = null;
  loading = true;
  error: string | null = null;
  logsDisplayedColumns: string[] = ['timestamp', 'status', 'log'];
  childrenDisplayedColumns: string[] = ['name', 'runId', 'type', 'actions'];

  constructor(
    private route: ActivatedRoute,
    private flowletApi: FlowletApi,
    private breadcrumbService: BreadcrumbService
  ) {}

  ngOnInit(): void {
    this.route.params.subscribe(params => {
      this.runId = params['id'];
      const parentId = this.route.snapshot.queryParamMap.get('parentId');

      if (parentId) {
        this.loadBreadcrumbsWithParent(this.runId, parentId);
      } else {
        this.loadBreadcrumbsSimple(this.runId);
      }

      this.loadRun();
    });
  }

  loadRun(): void {
    this.loading = true;
    this.error = null;
    this.flowletApi.getRun(this.runId).subscribe({
      next: (runDTO: RunDTO) => {
        this.runDTO = runDTO;
        this.run = this.transformRunDTO(runDTO);
        this.loading = false;
      },
      error: (err) => {
        this.error = 'Failed to load run: ' + err.message;
        this.loading = false;
      }
    });
  }

  private transformRunDTO(dto: RunDTO | any): RunDetailDisplay {
    // Transform RunDTO to match the template's expected structure
    return {
      run: {
        name: dto.span_name,
        run_id: dto.span_id,
        run_type: 'flow' // TODO: Get this from span_type if available
      },
      logs: (dto.logs || []).map((log: any) => ({
        timestamp: log.ts || '',
        status: log.level || '',
        log: log.message || ''
      })),
      parent: null, // Parent info not available in current RunDTO structure
      children: (dto.children || []).map((child: any) => this.transformRunDTO(child))
    };
  }

  private loadBreadcrumbsSimple(runId: string): void {
    // Set initial breadcrumbs
    this.breadcrumbService.setBreadcrumbs([
      { label: 'Home', url: '/' },
      { label: 'Runs', url: '/runs' },
      { label: 'Loading...', url: `/runs/${runId}` }
    ]);

    // Update with actual run name after data loads
    this.flowletApi.getRun(runId).subscribe({
      next: (runDTO: RunDTO) => {
        this.breadcrumbService.setBreadcrumbs([
          { label: 'Home', url: '/' },
          { label: 'Runs', url: '/runs' },
          { label: runDTO.span_name, url: `/runs/${runId}` }
        ]);
      },
      error: (err) => {
        this.breadcrumbService.setBreadcrumbs([
          { label: 'Home', url: '/' },
          { label: 'Runs', url: '/runs' },
          { label: 'Unknown Run', url: `/runs/${runId}` }
        ]);
        console.error('Failed to load run for breadcrumbs:', err);
      }
    });
  }

  private loadBreadcrumbsWithParent(runId: string, parentId: string): void {
    // Set initial breadcrumbs
    this.breadcrumbService.setBreadcrumbs([
      { label: 'Home', url: '/' },
      { label: 'Runs', url: '/runs' },
      { label: 'Loading...', url: `/runs/${parentId}` },
      { label: 'Loading...', url: `/runs/${runId}` }
    ]);

    // Fetch both parent and child run data
    forkJoin({
      parent: this.flowletApi.getRun(parentId),
      child: this.flowletApi.getRun(runId)
    }).subscribe({
      next: ({ parent, child }) => {
        this.breadcrumbService.setBreadcrumbs([
          { label: 'Home', url: '/' },
          { label: 'Runs', url: '/runs' },
          { label: parent.span_name, url: `/runs/${parentId}` },
          { label: child.span_name, url: `/runs/${runId}` }
        ]);
      },
      error: (err) => {
        // Fall back to simple breadcrumbs if parent fetch fails
        this.loadBreadcrumbsSimple(runId);
        console.error('Failed to load parent run for breadcrumbs:', err);
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
