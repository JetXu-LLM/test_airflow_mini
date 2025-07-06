/**
 * DAG Viewer component for visualizing workflows
 */

interface DAGViewProps {
    dagId: string;
    nodes: DAGNodeData[];
    edges: DAGEdgeData[];
    width?: number;
    height?: number;
    interactive?: boolean;
}

interface DAGNodeData {
    id: string;
    name: string;
    status: TaskStatus;
    type: string;
    position: { x: number; y: number };
    metadata?: Record<string, any>;
}

interface DAGEdgeData {
    id: string;
    source: string;
    target: string;
    type: 'dependency' | 'data_flow';
    label?: string;
}

enum ViewMode {
    GRAPH = 'graph',
    TREE = 'tree',
    LIST = 'list'
}

enum TaskStatus {
    PENDING = 'pending',
    RUNNING = 'running',
    SUCCESS = 'success',
    FAILED = 'failed',
    SKIPPED = 'skipped'
}

class DAGViewer {
    private container: HTMLElement;
    private props: DAGViewProps;
    private viewMode: ViewMode;
    private selectedNodes: Set<string>;
    
    constructor(container: HTMLElement, props: DAGViewProps) {
        this.container = container;
        this.props = props;
        this.viewMode = ViewMode.GRAPH;
        this.selectedNodes = new Set();
        
        this.initialize();
    }
    
    private initialize(): void {
        this.setupContainer();
        this.render();
        this.attachEventListeners();
    }
    
    private setupContainer(): void {
        this.container.style.width = `${this.props.width || 800}px`;
        this.container.style.height = `${this.props.height || 600}px`;
        this.container.style.border = '1px solid #ccc';
        this.container.style.position = 'relative';
    }
    
    public render(): void {
        this.container.innerHTML = '';
        
        switch (this.viewMode) {
            case ViewMode.GRAPH:
                this.renderGraphView();
                break;
            case ViewMode.TREE:
                this.renderTreeView();
                break;
            case ViewMode.LIST:
                this.renderListView();
                break;
        }
    }
    
    private renderGraphView(): void {
        const svg = document.createElementNS('http://www.w3.org/2000/svg', 'svg');
        svg.setAttribute('width', '100%');
        svg.setAttribute('height', '100%');
        
        // Render edges first (so they appear behind nodes)
        this.props.edges.forEach(edge => {
            const line = this.createEdgeElement(edge);
            svg.appendChild(line);
        });
        
        // Render nodes
        this.props.nodes.forEach(node => {
            const nodeGroup = this.createNodeElement(node);
            svg.appendChild(nodeGroup);
        });
        
        this.container.appendChild(svg);
    }
    
    private createNodeElement(node: DAGNodeData): SVGGElement {
        const group = document.createElementNS('http://www.w3.org/2000/svg', 'g');
        group.setAttribute('data-node-id', node.id);
        
        // Node circle
        const circle = document.createElementNS('http://www.w3.org/2000/svg', 'circle');
        circle.setAttribute('cx', node.position.x.toString());
        circle.setAttribute('cy', node.position.y.toString());
        circle.setAttribute('r', '20');
        circle.setAttribute('fill', this.getStatusColor(node.status));
        circle.setAttribute('stroke', '#333');
        circle.setAttribute('stroke-width', '2');
        
        // Node label
        const text = document.createElementNS('http://www.w3.org/2000/svg', 'text');
        text.setAttribute('x', node.position.x.toString());
        text.setAttribute('y', (node.position.y + 30).toString());
        text.setAttribute('text-anchor', 'middle');
        text.setAttribute('font-size', '12');
        text.textContent = node.name;
        
        group.appendChild(circle);
        group.appendChild(text);
        
        return group;
    }
    
    private createEdgeElement(edge: DAGEdgeData): SVGLineElement {
        const sourceNode = this.props.nodes.find(n => n.id === edge.source);
        const targetNode = this.props.nodes.find(n => n.id === edge.target);
        
        if (!sourceNode || !targetNode) {
            throw new Error(`Invalid edge: ${edge.source} -> ${edge.target}`);
        }
        
        const line = document.createElementNS('http://www.w3.org/2000/svg', 'line');
        line.setAttribute('x1', sourceNode.position.x.toString());
        line.setAttribute('y1', sourceNode.position.y.toString());
        line.setAttribute('x2', targetNode.position.x.toString());
        line.setAttribute('y2', targetNode.position.y.toString());
        line.setAttribute('stroke', '#666');
        line.setAttribute('stroke-width', '2');
        line.setAttribute('marker-end', 'url(#arrowhead)');
        
        return line;
    }
    
    private renderTreeView(): void {
        const treeContainer = document.createElement('div');
        treeContainer.className = 'dag-tree-view';
        
        const rootNodes = this.getRootNodes();
        rootNodes.forEach(node => {
            const treeNode = this.createTreeNode(node);
            treeContainer.appendChild(treeNode);
        });
        
        this.container.appendChild(treeContainer);
    }
    
    private renderListView(): void {
        const listContainer = document.createElement('div');
        listContainer.className = 'dag-list-view';
        
        this.props.nodes.forEach(node => {
            const listItem = this.createListItem(node);
            listContainer.appendChild(listItem);
        });
        
        this.container.appendChild(listContainer);
    }
    
    private createTreeNode(node: DAGNodeData): HTMLElement {
        const div = document.createElement('div');
        div.className = 'tree-node';
        div.innerHTML = `
            <span class="node-status ${node.status}">${node.name}</span>
            <span class="node-type">[${node.type}]</span>
        `;
        return div;
    }
    
    private createListItem(node: DAGNodeData): HTMLElement {
        const div = document.createElement('div');
        div.className = 'list-item';
        div.innerHTML = `
            <div class="item-header">
                <span class="node-name">${node.name}</span>
                <span class="node-status ${node.status}">${node.status}</span>
            </div>
            <div class="item-details">
                <span>Type: ${node.type}</span>
                <span>ID: ${node.id}</span>
            </div>
        `;
        return div;
    }
    
    private getRootNodes(): DAGNodeData[] {
        const hasIncomingEdge = new Set(this.props.edges.map(e => e.target));
        return this.props.nodes.filter(node => !hasIncomingEdge.has(node.id));
    }
    
    private getStatusColor(status: TaskStatus): string {
        const colors = {
            [TaskStatus.PENDING]: '#gray',
            [TaskStatus.RUNNING]: '#blue',
            [TaskStatus.SUCCESS]: '#green',
            [TaskStatus.FAILED]: '#red',
            [TaskStatus.SKIPPED]: '#orange'
        };
        return colors[status] || '#gray';
    }
    
    private attachEventListeners(): void {
        if (!this.props.interactive) return;
        
        this.container.addEventListener('click', (event) => {
            const target = event.target as Element;
            const nodeElement = target.closest('[data-node-id]');
            
            if (nodeElement) {
                const nodeId = nodeElement.getAttribute('data-node-id');
                if (nodeId) {
                    this.toggleNodeSelection(nodeId);
                }
            }
        });
    }
    
    private toggleNodeSelection(nodeId: string): void {
        if (this.selectedNodes.has(nodeId)) {
            this.selectedNodes.delete(nodeId);
        } else {
            this.selectedNodes.add(nodeId);
        }
        
        this.updateNodeStyles();
    }
    
    private updateNodeStyles(): void {
        const nodeElements = this.container.querySelectorAll('[data-node-id]');
        nodeElements.forEach(element => {
            const nodeId = element.getAttribute('data-node-id');
            if (nodeId && this.selectedNodes.has(nodeId)) {
                element.classList.add('selected');
            } else {
                element.classList.remove('selected');
            }
        });
    }
    
    public setViewMode(mode: ViewMode): void {
        this.viewMode = mode;
        this.render();
    }
    
    public getSelectedNodes(): string[] {
        return Array.from(this.selectedNodes);
    }
    
    public updateProps(newProps: Partial<DAGViewProps>): void {
        this.props = { ...this.props, ...newProps };
        this.render();
    }
}

// Utility functions
function renderDAG(containerId: string, dagData: DAGViewProps): DAGViewer {
    const container = document.getElementById(containerId);
    if (!container) {
        throw new Error(`Container with id '${containerId}' not found`);
    }
    
    return new DAGViewer(container, dagData);
}

function createSampleDAGData(): DAGViewProps {
    return {
        dagId: 'sample_dag',
        nodes: [
            {
                id: 'task1',
                name: 'Extract Data',
                status: TaskStatus.SUCCESS,
                type: 'python',
                position: { x: 100, y: 100 }
            },
            {
                id: 'task2',
                name: 'Transform Data',
                status: TaskStatus.RUNNING,
                type: 'python',
                position: { x: 300, y: 100 }
            },
            {
                id: 'task3',
                name: 'Load Data',
                status: TaskStatus.PENDING,
                type: 'sql',
                position: { x: 500, y: 100 }
            }
        ],
        edges: [
            {
                id: 'edge1',
                source: 'task1',
                target: 'task2',
                type: 'dependency'
            },
            {
                id: 'edge2',
                source: 'task2',
                target: 'task3',
                type: 'dependency'
            }
        ],
        width: 800,
        height: 400,
        interactive: true
    };
}

// Export for module usage
export { DAGViewer, ViewMode, TaskStatus, renderDAG, createSampleDAGData };
export type { DAGViewProps, DAGNodeData, DAGEdgeData };
