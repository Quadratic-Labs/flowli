import { Component, OnInit } from '@angular/core';
import { CommonModule } from '@angular/common';
import { ActivatedRoute, Router, RouterModule } from '@angular/router';
import { ReactiveFormsModule, FormBuilder, FormGroup, Validators } from '@angular/forms';
import { MatCardModule } from '@angular/material/card';
import { MatFormFieldModule } from '@angular/material/form-field';
import { MatInputModule } from '@angular/material/input';
import { MatButtonModule } from '@angular/material/button';
import { MatProgressSpinnerModule } from '@angular/material/progress-spinner';
import { MatSnackBar, MatSnackBarModule } from '@angular/material/snack-bar';
import { MatIconModule } from '@angular/material/icon';
import { MatCheckboxModule } from '@angular/material/checkbox';
import { FlowletApi, FlowSchema } from '../../services/flowlet-api';
import { BreadcrumbService } from '../../services/breadcrumb.service';

@Component({
  selector: 'app-flow-run',
  imports: [
    CommonModule,
    RouterModule,
    ReactiveFormsModule,
    MatCardModule,
    MatFormFieldModule,
    MatInputModule,
    MatButtonModule,
    MatProgressSpinnerModule,
    MatSnackBarModule,
    MatIconModule,
    MatCheckboxModule
  ],
  templateUrl: './flow-run.html',
  styleUrl: './flow-run.css',
})
export class FlowRun implements OnInit {
  flowName: string = '';
  flowSchema: FlowSchema | null = null;
  loading = true;
  submitting = false;
  error: string | null = null;
  argumentsForm: FormGroup;

  constructor(
    private route: ActivatedRoute,
    private router: Router,
    private flowletApi: FlowletApi,
    private formBuilder: FormBuilder,
    private snackBar: MatSnackBar,
    private breadcrumbService: BreadcrumbService
  ) {
    this.argumentsForm = this.formBuilder.group({});
  }

  ngOnInit(): void {
    this.flowName = this.route.snapshot.paramMap.get('name') || '';
    if (!this.flowName) {
      this.error = 'Flow name not provided';
      this.loading = false;
      return;
    }

    this.breadcrumbService.setBreadcrumbs([
      { label: 'Home', url: '/' },
      { label: 'Flows', url: '/flows' },
      { label: `Run Flow: ${this.flowName}`, url: `/flows/${this.flowName}/run` }
    ]);

    this.loadFlowSchema();
  }

  loadFlowSchema(): void {
    this.loading = true;
    this.error = null;

    this.flowletApi.getFlows().subscribe({
      next: (flows) => {
        const flow = flows.find(f => f.name === this.flowName);
        if (!flow) {
          this.error = `Flow "${this.flowName}" not found`;
          this.loading = false;
          return;
        }

        this.flowSchema = flow;
        this.buildForm();
        this.loading = false;
      },
      error: (err) => {
        this.error = 'Failed to load flow schema: ' + err.message;
        this.loading = false;
      }
    });
  }

  buildForm(): void {
    if (!this.flowSchema || !this.flowSchema.parameters) {
      return;
    }

    const group: any = {};

    this.flowSchema.parameters.forEach(param => {
      const validators = param.required ? [Validators.required] : [];
      const defaultValue = this.getDefaultValue(param);
      group[param.name] = [defaultValue, validators];
    });

    this.argumentsForm = this.formBuilder.group(group);
  }

  getDefaultValue(param: any): any {
    // Check if parameter has a default value
    if (param.default !== undefined && param.default !== null) {
      return param.default;
    }

    // Return appropriate default based on type
    switch (param.type.toLowerCase()) {
      case 'bool':
      case 'boolean':
        return false;
      case 'int':
      case 'float':
      case 'number':
        return null;
      case 'str':
      case 'string':
        return '';
      default:
        return '';
    }
  }

  getInputType(paramType: string): string {
    const lowerType = paramType.toLowerCase();
    if (lowerType.includes('int') || lowerType.includes('float') || lowerType === 'number') {
      return 'number';
    }
    return 'text';
  }

  isBooleanType(paramType: string): boolean {
    const lowerType = paramType.toLowerCase();
    return lowerType === 'bool' || lowerType === 'boolean';
  }

  onSubmit(): void {
    if (this.argumentsForm.invalid) {
      this.snackBar.open('Please fill in all required fields', 'Close', { duration: 3000 });
      return;
    }

    this.submitting = true;
    const kwargs = this.prepareKwargs();

    this.flowletApi.submitFlow(this.flowName, { kwargs }).subscribe({
      next: () => {
        this.snackBar.open(`Flow "${this.flowName}" submitted for execution`, 'Close', { duration: 3000 });
        this.submitting = false;
        // Navigate to runs list
        this.router.navigate(['/runs']);
      },
      error: (err) => {
        this.snackBar.open('Failed to submit flow: ' + err.message, 'Close', { duration: 5000 });
        this.submitting = false;
      }
    });
  }

  prepareKwargs(): { [key: string]: any } {
    const formValue = this.argumentsForm.value;
    const kwargs: { [key: string]: any } = {};

    // Only include non-empty values
    Object.keys(formValue).forEach(key => {
      const value = formValue[key];
      if (value !== null && value !== '') {
        kwargs[key] = value;
      }
    });

    return kwargs;
  }
}
