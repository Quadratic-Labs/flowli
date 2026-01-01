import { Component, Input, OnInit, ElementRef, ViewChild, OnChanges, SimpleChanges } from '@angular/core';
import { CommonModule } from '@angular/common';
import * as d3 from 'd3';
import { RunSummaryDTO } from '../../services/flowlet-api';

interface FlamegraphNode {
  name: string;
  start: number;
  end: number;
  duration: number;
  status: string;
  depth: number;
  children: FlamegraphNode[];
  spanId: string;
}

@Component({
  selector: 'app-run-flamegraph',
  imports: [CommonModule],
  templateUrl: './run-flamegraph.html',
  styleUrl: './run-flamegraph.css',
})
export class RunFlamegraph implements OnInit, OnChanges {
  @Input() runData!: RunSummaryDTO;
  @ViewChild('flamegraphContainer', { static: true }) containerRef!: ElementRef;

  private svg: any;
  private width = 0;
  private height = 400;
  private margin = { top: 20, right: 20, bottom: 40, left: 60 };

  ngOnInit(): void {
    this.initializeChart();
  }

  ngOnChanges(changes: SimpleChanges): void {
    if (changes['runData'] && !changes['runData'].firstChange) {
      this.updateChart();
    }
  }

  private initializeChart(): void {
    if (!this.runData) return;

    // Get container width
    const container = this.containerRef.nativeElement;
    this.width = container.offsetWidth - this.margin.left - this.margin.right;

    // Clear any existing SVG
    d3.select(this.containerRef.nativeElement).select('svg').remove();

    // Create SVG
    this.svg = d3.select(this.containerRef.nativeElement)
      .append('svg')
      .attr('width', this.width + this.margin.left + this.margin.right)
      .attr('height', this.height + this.margin.top + this.margin.bottom)
      .append('g')
      .attr('transform', `translate(${this.margin.left},${this.margin.top})`);

    this.updateChart();
  }

  private updateChart(): void {
    if (!this.runData || !this.svg) return;

    // Transform data into flamegraph nodes
    const rootNode = this.transformToFlamegraphNode(this.runData, 0);

    // Calculate the total duration and start time
    const totalStart = rootNode.start;
    const totalEnd = rootNode.end;
    const totalDuration = totalEnd - totalStart;

    // Create scales
    const xScale = d3.scaleLinear()
      .domain([0, totalDuration])
      .range([0, this.width]);

    // Calculate max depth for y-scale
    const maxDepth = this.calculateMaxDepth(rootNode);
    const barHeight = Math.min(40, this.height / (maxDepth + 1));

    const yScale = d3.scaleLinear()
      .domain([0, maxDepth + 1])
      .range([0, (maxDepth + 1) * barHeight]);

    // Clear existing content
    this.svg.selectAll('*').remove();

    // Create tooltip
    const tooltip = d3.select('body').append('div')
      .attr('class', 'flamegraph-tooltip')
      .style('position', 'absolute')
      .style('visibility', 'hidden')
      .style('background-color', 'rgba(0, 0, 0, 0.8)')
      .style('color', 'white')
      .style('padding', '8px')
      .style('border-radius', '4px')
      .style('font-size', '12px')
      .style('pointer-events', 'none')
      .style('z-index', '1000');

    // Flatten tree for rendering
    const nodes = this.flattenTree(rootNode);

    // Create bars
    const bars = this.svg.selectAll('.bar')
      .data(nodes)
      .enter()
      .append('g')
      .attr('class', 'bar');

    bars.append('rect')
      .attr('x', (d: FlamegraphNode) => xScale(d.start - totalStart))
      .attr('y', (d: FlamegraphNode) => this.height - (d.depth + 1) * barHeight)
      .attr('width', (d: FlamegraphNode) => Math.max(1, xScale(d.duration)))
      .attr('height', barHeight - 2)
      .attr('fill', (d: FlamegraphNode) => this.getStatusColor(d.status))
      .attr('stroke', '#fff')
      .attr('stroke-width', 1)
      .style('cursor', 'pointer')
      .on('mouseover', (event: any, d: FlamegraphNode) => {
        d3.select(event.currentTarget).attr('opacity', 0.7);
        tooltip
          .style('visibility', 'visible')
          .html(`
            <strong>${d.name}</strong><br/>
            Status: ${d.status}<br/>
            Duration: ${d.duration.toFixed(2)}ms<br/>
            Depth: ${d.depth}
          `);
      })
      .on('mousemove', (event: any) => {
        tooltip
          .style('top', (event.pageY - 10) + 'px')
          .style('left', (event.pageX + 10) + 'px');
      })
      .on('mouseout', (event: any) => {
        d3.select(event.currentTarget).attr('opacity', 1);
        tooltip.style('visibility', 'hidden');
      });

    // Add text labels (only if bar is wide enough)
    bars.append('text')
      .attr('x', (d: FlamegraphNode) => xScale(d.start - totalStart) + 4)
      .attr('y', (d: FlamegraphNode) => this.height - (d.depth + 1) * barHeight + barHeight / 2)
      .attr('dy', '0.35em')
      .attr('fill', '#fff')
      .attr('font-size', '11px')
      .style('pointer-events', 'none')
      .text((d: FlamegraphNode) => {
        const barWidth = xScale(d.duration);
        return barWidth > 50 ? d.name : '';
      });

    // Add X-axis at the bottom
    const xAxis = d3.axisBottom(xScale)
      .ticks(10)
      .tickFormat((d: any) => `${d}ms`);

    this.svg.append('g')
      .attr('transform', `translate(0,${this.height})`)
      .call(xAxis)
      .append('text')
      .attr('x', this.width / 2)
      .attr('y', 35)
      .attr('fill', '#000')
      .attr('text-anchor', 'middle')
      .text('Elapsed Time (ms)');

    // Add Y-axis label (centered on the chart area)
    this.svg.append('text')
      .attr('transform', 'rotate(-90)')
      .attr('y', -40)
      .attr('x', -this.height / 2)
      .attr('fill', '#000')
      .attr('text-anchor', 'middle')
      .text('Call Depth');
  }

  private transformToFlamegraphNode(dto: RunSummaryDTO, depth: number): FlamegraphNode {
    const start = new Date(dto.start_ts).getTime();
    const end = dto.end_ts ? new Date(dto.end_ts).getTime() : start;
    const duration = end - start;

    return {
      name: dto.span_name,
      start: start,
      end: end,
      duration: duration,
      status: dto.status,
      depth: depth,
      spanId: dto.span_id,
      children: (dto.children || []).map(child => this.transformToFlamegraphNode(child, depth + 1))
    };
  }

  private calculateMaxDepth(node: FlamegraphNode): number {
    if (!node.children || node.children.length === 0) {
      return node.depth;
    }
    return Math.max(...node.children.map(child => this.calculateMaxDepth(child)));
  }

  private flattenTree(node: FlamegraphNode): FlamegraphNode[] {
    const result: FlamegraphNode[] = [node];
    if (node.children) {
      node.children.forEach(child => {
        result.push(...this.flattenTree(child));
      });
    }
    return result;
  }

  private getStatusColor(status: string): string {
    const statusLower = status.toLowerCase();
    switch(statusLower) {
      case 'success':
      case 'completed':
        return '#4caf50'; // Green
      case 'failed':
      case 'error':
      case 'critical':
        return '#f44336'; // Red
      case 'running':
      case 'in_progress':
        return '#2196f3'; // Blue
      case 'warning':
        return '#ff9800'; // Orange
      default:
        return '#9e9e9e'; // Grey
    }
  }
}
