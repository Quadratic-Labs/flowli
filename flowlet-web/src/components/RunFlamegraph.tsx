import { useRef, useEffect } from 'react';
import * as d3 from 'd3';
import type { TraceSummaryDTO } from '@/lib/api';
import { getStatusColor } from '@/lib/api';

interface FNode { name: string; start: number; end: number; duration: number; status: string; depth: number; spanId: string; children: FNode[]; }

function toFNode(dto: TraceSummaryDTO, depth: number): FNode {
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

const MARGIN = { top: 20, right: 20, bottom: 40, left: 60 };
const HEIGHT = 400;
const ENTER_MS = 400;

/** Draws the span tree as a flamegraph, updating in place: the SVG scaffold
 *  is built once, and data changes flow through a keyed join (span_id) with
 *  transitions — new bars grow in, moved bars glide, vanished bars fade out.
 *  Unchanged data (same reference) is never redrawn, so poll-driven parent
 *  re-renders cost nothing. */
export function RunFlamegraph({ runData }: { runData: TraceSummaryDTO }) {
  const containerRef = useRef<HTMLDivElement>(null);
  const tooltipRef = useRef<d3.Selection<HTMLDivElement, unknown, HTMLElement, unknown> | null>(null);

  // The tooltip lives on <body>, once per component instance; bar handlers
  // close over this singleton so it must outlive individual data updates.
  useEffect(() => {
    tooltipRef.current = d3.select('body').append('div')
      .style('position', 'absolute').style('visibility', 'hidden')
      .style('background', 'rgba(0,0,0,0.8)').style('color', '#fff')
      .style('padding', '8px').style('border-radius', '4px')
      .style('font-size', '12px').style('pointer-events', 'none').style('z-index', '9999');
    return () => { tooltipRef.current?.remove(); tooltipRef.current = null; };
  }, []);

  useEffect(() => {
    if (!containerRef.current || !runData) return;
    const container = containerRef.current;
    const width = container.offsetWidth - MARGIN.left - MARGIN.right;

    // Static scaffolding: created on first draw, reused afterwards.
    let svg = d3.select(container).select<SVGSVGElement>('svg');
    if (svg.empty()) {
      svg = d3.select(container).append('svg');
      const g = svg.append('g').attr('class', 'chart')
        .attr('transform', `translate(${MARGIN.left},${MARGIN.top})`);
      g.append('g').attr('class', 'bars');
      g.append('g').attr('class', 'x-axis').attr('transform', `translate(0,${HEIGHT})`);
      g.append('text').attr('class', 'x-label')
        .attr('y', HEIGHT + 35).attr('fill', '#000').attr('text-anchor', 'middle')
        .text('Elapsed Time (ms)');
      g.append('text').attr('transform', 'rotate(-90)').attr('y', -40).attr('x', -HEIGHT / 2)
        .attr('fill', '#000').attr('text-anchor', 'middle').text('Call Depth');
    }
    svg.attr('width', width + MARGIN.left + MARGIN.right)
      .attr('height', HEIGHT + MARGIN.top + MARGIN.bottom);
    svg.select('.x-label').attr('x', width / 2);

    const root = toFNode(runData, 0);
    const totalStart = root.start;
    const totalDuration = root.end - root.start;
    const md = maxDepth(root);
    const barH = Math.min(40, HEIGHT / (md + 1));
    const xScale = d3.scaleLinear().domain([0, totalDuration]).range([0, width]);
    const nodes = flatten(root);

    const barX = (d: FNode) => xScale(d.start - totalStart);
    const barY = (d: FNode) => HEIGHT - (d.depth + 1) * barH;
    const barW = (d: FNode) => Math.max(1, xScale(d.duration));

    const bars = svg.select<SVGGElement>('.bars')
      .selectAll<SVGGElement, FNode>('g.bar')
      .data(nodes, d => d.spanId)
      .join(
        enter => {
          const ge = enter.append('g').attr('class', 'bar');
          // New bars grow from zero width at their final position.
          ge.append('rect')
            .attr('x', barX).attr('y', barY)
            .attr('width', 0).attr('height', barH - 2)
            .attr('fill', d => getStatusColor(d.status))
            .attr('stroke', '#fff').attr('stroke-width', 1)
            .on('mouseover', function(_event, d) {
              d3.select(this).attr('opacity', 0.7);
              tooltipRef.current?.style('visibility', 'visible')
                .html(`<strong>${d.name}</strong><br/>Status: ${d.status}<br/>Duration: ${d.duration.toFixed(2)}ms<br/>Depth: ${d.depth}`);
            })
            .on('mousemove', (event: MouseEvent) => {
              tooltipRef.current?.style('top', (event.pageY - 10) + 'px').style('left', (event.pageX + 10) + 'px');
            })
            .on('mouseout', function() {
              d3.select(this).attr('opacity', 1);
              tooltipRef.current?.style('visibility', 'hidden');
            });
          ge.append('text')
            .attr('x', d => barX(d) + 4)
            .attr('y', d => barY(d) + barH / 2)
            .attr('dy', '0.35em').attr('fill', '#fff').attr('font-size', '11px')
            .style('pointer-events', 'none').style('opacity', 0);
          return ge;
        },
        update => update,
        exit => exit.transition().duration(ENTER_MS).style('opacity', 0).remove(),
      );

    // Enter and update share one transition to their current geometry, so
    // an incremental refresh moves/extends bars instead of repainting them.
    bars.select<SVGRectElement>('rect').transition().duration(ENTER_MS)
      .attr('x', barX).attr('y', barY)
      .attr('width', barW).attr('height', barH - 2)
      .attr('fill', d => getStatusColor(d.status));
    bars.select<SVGTextElement>('text')
      .text(d => barW(d) > 50 ? d.name : '')
      .transition().duration(ENTER_MS)
      .attr('x', d => barX(d) + 4)
      .attr('y', d => barY(d) + barH / 2)
      .style('opacity', 1);

    svg.select<SVGGElement>('.x-axis').transition().duration(ENTER_MS)
      .call(d3.axisBottom(xScale).ticks(10).tickFormat(d => `${d}ms`));
  }, [runData]);

  return <div ref={containerRef} className="w-full" />;
}
