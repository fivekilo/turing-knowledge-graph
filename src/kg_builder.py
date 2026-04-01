#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
图灵知识图谱构建脚本
Turing Knowledge Graph Builder

作者：李嘉轩
日期：2026-04-01
"""

import networkx as nx
import matplotlib.pyplot as plt
from datetime import datetime


class TuringKnowledgeGraph:
    """图灵知识图谱构建类"""
    
    def __init__(self):
        self.graph = nx.DiGraph()
        self.nodes = []
        self.edges = []
    
    def add_node(self, node_id: str, label: str, node_type: str, properties: dict = None):
        """添加节点"""
        self.graph.add_node(node_id, label=label, type=node_type, **(properties or {}))
        self.nodes.append({
            'id': node_id,
            'label': label,
            'type': node_type,
            'properties': properties or {}
        })
    
    def add_edge(self, source: str, target: str, relation: str, properties: dict = None):
        """添加边（关系）"""
        self.graph.add_edge(source, target, relation=relation, **(properties or {}))
        self.edges.append({
            'source': source,
            'target': target,
            'relation': relation,
            'properties': properties or {}
        })
    
    def build_basic_turing_kg(self):
        """构建基础的图灵知识图谱"""
        
        # ========== 节点 ==========
        # 人物
        self.add_node('turing', 'Alan Turing', 'Person', {
            'birth': '1912-06-23',
            'death': '1954-06-07',
            'nationality': 'British',
            'occupation': 'Mathematician, Computer Scientist, Logician'
        })
        
        # 机构
        self.add_node('bletchley_park', 'Bletchley Park', 'Organization', {
            'location': 'Milton Keynes, England',
            'type': 'Codebreaking Center'
        })
        
        self.add_node('cam', 'University of Cambridge', 'Organization', {
            'location': 'Cambridge, England',
            'type': 'University'
        })
        
        self.add_node('princeton', 'Princeton University', 'Organization', {
            'location': 'New Jersey, USA',
            'type': 'University'
        })
        
        # 重要概念
        self.add_node('turing_machine', 'Turing Machine', 'Concept', {
            'year': '1936',
            'field': 'Computer Science'
        })
        
        self.add_node('enigma', 'Enigma Machine', 'Concept', {
            'type': 'Cipher Device',
            'origin': 'Germany'
        })
        
        self.add_node('turing_test', 'Turing Test', 'Concept', {
            'year': '1950',
            'field': 'Artificial Intelligence'
        })
        
        self.add_node('ace', 'Automatic Computing Engine (ACE)', 'Concept', {
            'year': '1945',
            'type': 'Computer Design'
        })
        
        # 著作/论文
        self.add_node('on_computable', 'On Computable Numbers', 'Work', {
            'year': '1936',
            'journal': 'Proceedings of the London Mathematical Society'
        })
        
        self.add_node('computing_machinery', 'Computing Machinery and Intelligence', 'Work', {
            'year': '1950',
            'journal': 'Mind'
        })
        
        # ========== 关系（边） ==========
        # 教育关系
        self.add_edge('turing', 'cam', 'studied_at', {'department': 'Mathematics'})
        self.add_edge('turing', 'princeton', 'studied_at', {'degree': 'PhD', 'year': '1938'})
        
        # 工作关系
        self.add_edge('turing', 'bletchley_park', 'worked_at', {'year': '1938-1942', 'role': 'Cryptanalyst'})
        
        # 贡献关系
        self.add_edge('turing', 'turing_machine', 'invented', {'year': '1936'})
        self.add_edge('turing', 'turing_test', 'proposed', {'year': '1950'})
        self.add_edge('turing', 'enigma', 'broke', {'year': '1942', 'role': 'Bombe development'})
        self.add_edge('turing', 'ace', 'designed', {'year': '1945'})
        
        # 论文关系
        self.add_edge('turing', 'on_computable', 'wrote', {'year': '1936'})
        self.add_edge('turing', 'computing_machinery', 'wrote', {'year': '1950'})
        
        # 概念关联
        self.add_edge('turing_machine', 'computer_science', 'foundation_of', {})
        self.add_edge('turing_test', 'ai', 'foundation_of', {})
        self.add_edge('enigma', 'wwii', 'used_in', {})
        
        print(f"✅ 知识图谱构建完成！")
        print(f"   - 节点数: {len(self.nodes)}")
        print(f"   - 边数: {len(self.edges)}")
    
    def visualize(self, output_path: str = None):
        """可视化知识图谱"""
        plt.figure(figsize=(16, 12))
        
        # 按类型给节点着色
        node_colors = []
        color_map = {
            'Person': '#FF6B6B',
            'Organization': '#4ECDC4',
            'Concept': '#45B7D1',
            'Work': '#96CEB4'
        }
        
        for node in self.graph.nodes():
            node_type = self.graph.nodes[node].get('type', 'Concept')
            node_colors.append(color_map.get(node_type, '#95A5A6'))
        
        # 布局
        pos = nx.spring_layout(self.graph, k=2, iterations=50, seed=42)
        
        # 绘制
        nx.draw(self.graph, pos,
                with_labels=True,
                node_color=node_colors,
                node_size=2500,
                font_size=10,
                font_weight='bold',
                arrows=True,
                arrowsize=20,
                edge_color='#555555',
                width=2,
                alpha=0.9)
        
        plt.title("Alan Turing Knowledge Graph | 图灵知识图谱", fontsize=16, fontweight='bold')
        plt.axis('off')
        
        if output_path:
            plt.savefig(output_path, dpi=300, bbox_inches='tight', facecolor='white')
            print(f"📊 知识图谱已保存至: {output_path}")
        else:
            plt.show()
    
    def save(self, nodes_path: str = 'data/nodes.csv', edges_path: str = 'data/edges.csv'):
        """保存知识图谱数据"""
        import pandas as pd
        
        # 保存节点
        if self.nodes:
            pd.DataFrame(self.nodes).to_csv(nodes_path, index=False, encoding='utf-8')
            print(f"📄 节点数据已保存至: {nodes_path}")
        
        # 保存边
        if self.edges:
            pd.DataFrame(self.edges).to_csv(edges_path, index=False, encoding='utf-8')
            print(f"📄 边数据已保存至: {edges_path}")
    
    def export_rdf(self, output_path: str = 'data/turing_kg.rdf'):
        """导出为 RDF 格式"""
        from rdflib import Graph, Namespace, URIRef, Literal, RDF, RDFS
        
        g = Graph()
        
        # 定义命名空间
        TURING = Namespace("http://example.org/turing/")
        g.bind("turing", TURING)
        
        # 添加节点
        for node in self.nodes:
            subject = TURING[node['id']]
            g.add((subject, RDF.type, URIRef(f"http://example.org/turing/{node['type']}")))
            g.add((subject, RDFS.label, Literal(node['label'])))
        
        # 添加边
        for edge in self.edges:
            subject = TURING[edge['source']]
            obj = TURING[edge['target']]
            predicate = URIRef(f"http://example.org/turing/{edge['relation']}")
            g.add((subject, predicate, obj))
        
        g.serialize(output_path, format='xml')
        print(f"📚 RDF 数据已保存至: {output_path}")


def main():
    """主函数"""
    print("=" * 50)
    print("🧠 图灵知识图谱构建器")
    print("   Turing Knowledge Graph Builder")
    print("=" * 50)
    
    # 创建知识图谱
    kg = TuringKnowledgeGraph()
    kg.build_basic_turing_kg()
    
    # 保存数据
    kg.save()
    
    # 导出 RDF
    kg.export_rdf()
    
    # 可视化
    kg.visualize('data/turing_kg.png')
    
    print("\n✅ 所有任务完成！")
    return kg


if __name__ == "__main__":
    main()
