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
    
    private renderTreeView(): void {
        const treeContainer = document.createElement('div');
        treeContainer.className = 'dag-tree-view';
        
        // Simple tree rendering logic
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
        line.
