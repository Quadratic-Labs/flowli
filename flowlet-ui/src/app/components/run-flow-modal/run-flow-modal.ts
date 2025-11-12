import { Component, Input } from '@angular/core';
import { CommonModule } from '@angular/common';
import { FormsModule } from '@angular/forms';
import { NgbActiveModal } from '@ng-bootstrap/ng-bootstrap';

interface FlowArgument {
  key: string;
  value: any;
  type: 'string' | 'number' | 'boolean';
}

@Component({
  selector: 'app-run-flow-modal',
  imports: [CommonModule, FormsModule],
  templateUrl: './run-flow-modal.html',
  styleUrl: './run-flow-modal.css',
})
export class RunFlowModal {
  @Input() flowName: string = '';

  arguments: FlowArgument[] = [];

  constructor(public activeModal: NgbActiveModal) {}

  addArgument(): void {
    this.arguments.push({ key: '', value: '', type: 'string' });
  }

  removeArgument(index: number): void {
    this.arguments.splice(index, 1);
  }

  onTypeChange(arg: FlowArgument): void {
    // Convert value based on type
    if (arg.type === 'boolean') {
      arg.value = false;
    } else if (arg.type === 'number') {
      arg.value = 0;
    } else {
      arg.value = '';
    }
  }

  getKwargs(): { [key: string]: any } {
    const kwargs: { [key: string]: any } = {};

    for (const arg of this.arguments) {
      if (arg.key.trim()) {
        let value = arg.value;

        // Convert string to appropriate type
        if (arg.type === 'number') {
          value = parseFloat(arg.value) || 0;
        } else if (arg.type === 'boolean') {
          value = arg.value === true || arg.value === 'true';
        }

        kwargs[arg.key.trim()] = value;
      }
    }

    return kwargs;
  }

  submit(): void {
    const kwargs = this.getKwargs();
    this.activeModal.close(kwargs);
  }

  cancel(): void {
    this.activeModal.dismiss('cancel');
  }
}
