import { useRef, useEffect } from 'react';
import * as d3 from 'd3';
import type { RunSummaryDTO } from '@/lib/api';
import { getStatusColor } from '@/lib/api';

interface FNode { name: string; start: number; end: number; duration: number; status: string; depth: number; spanId: string; children: FNode[]; }

function toFNode(dto: RunSummaryDTO, depth: number): FNode {
  const start = new Date(dto.start_ts).getTime();
  const end = dto.end_ts ? new Date(dto.end_ts).getTime() : start;
  return { name: dto.span_name, start, end, duration: end - start, status: dto.status, depth, spanId: dto.span_id,
    children: (dto.children || []).map(c => toFNode(c, depth + 1)) };
}

function flatten(node: FNode): FNode[] {
  return [node, ...node.children.flatMap(flatten)];
}

function maxDepth(node: FNode): number {
  if (!node.children.length) return node.depth;
  return Math.max(...node.children.map(maxDepth));
}

export function RunFlamegraph({ runData }: { runData: RunSummaryDTO }) {
  const containerRef = useRef<HTMLDivElement>(null);
  const tooltipRef = useRef<d3.Selection<HTMLDivElement, unknown, HTMLElement, unknown> | null>(null);

  useEffect(() => {
    if (!containerRef.current || !runData) return;
    const container = containerRef.current;
    const margin = { top: 20, right: 20, bottom: 40, left: 60 };
    const width = container.offsetWidth - margin.left - margin.right;
    const height = 400;

    d3.select(container).select('svg').remove();
    if (tooltipRef.current) { tooltipRef.current.remove(); tooltipRef.current = null; }

    const svg = d3.select(container).append('svg')
      .attr('width', width + margin.left + margin.right)
      .attr('height', height + margin.top + margin.bottom)
      .append('g').attr('transform', `translate(${margin.left},${margin.top})`);

    const root = toFNode(runData, 0);
    const totalStart = root.start;
    const totalDuration = root.end - root.start;
    const md = maxDepth(root);
    const barH = Math.min(40, height / (md + 1));

    const xScale = d3.scaleLinear().domain([0, totalDuration]).range([0, width]);

    const tooltip = d3.select('body').append('div')
      .style('position', 'absolute').style('visibility', 'hidden')
      .style('background', 'rgba(0,0,0,0.8)').style('color', '#fff')
      .style('padding', '8px').style('border-radius', '4px')
      .style('font-size', '12px').style('pointer-events', 'none').style('z-index', '9999');
    tooltipRef.current = tooltip;

    const nodes = flatten(root);
    const bars = svg.selectAll('.bar').data(nodes).enter().append('g').attr('class', 'bar');

    bars.append('rect')
      .attr('x', d => xScale(d.start - totalStart))
      .attr('y', d => height - (d.depth + 1) * barH)
      .attr('width', d => Math.max(1, xScale(d.duration)))
      .attr('height', barH - 2)
      .attr('fill', d => getStatusColor(d.status))
      .attr('stroke', '#fff').attr('stroke-width', 1)
      .on('mouseover', function(_event, d) {
        d3.select(this).attr('opacity', 0.7);
        tooltip.style('visibility', 'visible')
          .html(`<strong>${d.name}</strong><br/>Status: ${d.status}<br/>Duration: ${d.duration.toFixed(2)}ms<br/>Depth: ${d.depth}`);
      })
      .on('mousemove', (_event: MouseEvent, _d: FNode) => {
        const e = _event as MouseEvent;
        tooltip.style('top', (e.pageY - 10) + 'px').style('left', (e.pageX + 10) + 'px');
      })
      .on('mouseout', function() { d3.select(this).attr('opacity', 1); tooltip.style('visibility', 'hidden'); });

    bars.append('text')
      .attr('x', d => xScale(d.start - totalStart) + 4)
      .attr('y', d => height - (d.depth + 1) * barH + barH / 2)
      .attr('dy', '0.35em').attr('fill', '#fff').attr('font-size', '11px')
      .style('pointer-events', 'none')
      .text(d => xScale(d.duration) > 50 ? d.name : '');

    svg.append('g').attr('transform', `translate(0,${height})`)
      .call(d3.axisBottom(xScale).ticks(10).tickFormat(d => `${d}ms`))
      .append('text').attr('x', width / 2).attr('y', 35)
      .attr('fill', '#000').attr('text-anchor', 'middle').text('Elapsed Time (ms)');

    svg.append('text').attr('transform', 'rotate(-90)').attr('y', -40).attr('x', -height / 2)
      .attr('fill', '#000').attr('text-anchor', 'middle').text('Call Depth');

    return () => { tooltip.remove(); };
  }, [runData]);

  return <div ref={containerRef} className="w-full" />;
}
