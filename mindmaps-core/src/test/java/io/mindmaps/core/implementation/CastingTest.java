/*
 * MindmapsDB - A Distributed Semantic Database
 * Copyright (C) 2016  Mindmaps Research Ltd
 *
 * MindmapsDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * MindmapsDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with MindmapsDB. If not, see <http://www.gnu.org/licenses/gpl.txt>.
 */

package io.mindmaps.core.implementation;

import io.mindmaps.constants.DataType;
import io.mindmaps.core.implementation.exception.MoreThanOneEdgeException;
import io.mindmaps.core.implementation.exception.NoEdgeException;
import io.mindmaps.core.model.Concept;
import io.mindmaps.core.model.EntityType;
import io.mindmaps.core.model.Relation;
import io.mindmaps.factory.MindmapsTestGraphFactory;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.UUID;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

public class CastingTest {

    private MindmapsTransactionImpl mindmapsGraph;
    private CastingImpl casting;
    private RoleTypeImpl role;
    private RelationImpl relation;
    private InstanceImpl rolePlayer;

    @Rule
    public final ExpectedException expectedException = ExpectedException.none();

    @Before
    public void setUp() {
        mindmapsGraph = (MindmapsTransactionImpl) MindmapsTestGraphFactory.newEmptyGraph().getTransaction();
        mindmapsGraph.initialiseMetaConcepts();
        role = (RoleTypeImpl) mindmapsGraph.putRoleType("Role");
        EntityTypeImpl conceptType = (EntityTypeImpl) mindmapsGraph.putEntityType("A thing");
        rolePlayer = (InstanceImpl) mindmapsGraph.putEntity("role player main", conceptType);
        RelationTypeImpl relationType = (RelationTypeImpl) mindmapsGraph.putRelationType("A type");
        relation = (RelationImpl) mindmapsGraph.putRelation("a relation", relationType);
        casting = mindmapsGraph.putCasting(role, rolePlayer, relation);
    }
    @After
    public void destroyGraphAccessManager() throws Exception {
        mindmapsGraph.close();
    }

    @Test
    public void testEquals() throws Exception {
        Graph graph = mindmapsGraph.getTinkerPopGraph();
        Vertex v = graph.traversal().V(relation.getBaseIdentifier()).out(DataType.EdgeLabel.CASTING.getLabel()).next();
        CastingImpl castingCopy = (CastingImpl) mindmapsGraph.getConcept(v.value(DataType.ConceptPropertyUnique.ITEM_IDENTIFIER.name()));
        assertEquals(casting, castingCopy);

        EntityType type = mindmapsGraph.putEntityType("Another entity type");
        RoleTypeImpl role = (RoleTypeImpl) mindmapsGraph.putRoleType("Role 2");
        InstanceImpl rolePlayer = (InstanceImpl) mindmapsGraph.putEntity("An instance", type);
        CastingImpl casting2 = mindmapsGraph.putCasting(role, rolePlayer, relation);
        assertNotEquals(casting, casting2);
    }

    @Test
    public void hashCodeTest() throws Exception {
        Vertex castingVertex = mindmapsGraph.getTinkerPopGraph().traversal().V(casting.getBaseIdentifier()).next();
        assertEquals(casting.hashCode(), castingVertex.hashCode());
    }

    @Test
    public void testGetRole() throws Exception {
        assertEquals(role, casting.getRole());

        String id = UUID.randomUUID().toString();
        Vertex vertex = mindmapsGraph.getTinkerPopGraph().addVertex(DataType.BaseType.CASTING.name());
        vertex.property(DataType.ConceptPropertyUnique.ITEM_IDENTIFIER.name(), id);

        CastingImpl casting2 = (CastingImpl) mindmapsGraph.getConcept(id);
        boolean exceptionThrown = false;
        try{
            casting2.getRole();
        } catch(NoEdgeException e){
            exceptionThrown = true;
        }
        assertTrue(exceptionThrown);


        TypeImpl c1 = (TypeImpl) mindmapsGraph.putEntityType("c1'");
        TypeImpl c2 = (TypeImpl) mindmapsGraph.putEntityType("c2");
        Vertex casting2_Vertex = mindmapsGraph.getTinkerPopGraph().traversal().V(casting2.getBaseIdentifier()).next();
        Vertex c1_Vertex = mindmapsGraph.getTinkerPopGraph().traversal().V(c1.getBaseIdentifier()).next();
        Vertex c2_Vertex = mindmapsGraph.getTinkerPopGraph().traversal().V(c2.getBaseIdentifier()).next();

        casting2_Vertex.addEdge(DataType.EdgeLabel.ISA.getLabel(), c1_Vertex);
        casting2_Vertex.addEdge(DataType.EdgeLabel.ISA.getLabel(), c2_Vertex);

        exceptionThrown = false;
        try{
            casting2.getRole();
        } catch(MoreThanOneEdgeException e){
            exceptionThrown = true;
        }
        assertTrue(exceptionThrown);
    }

    @Test
    public void testGetRolePlayer() throws Exception {
        assertEquals(rolePlayer, casting.getRolePlayer());
    }

    @Test (expected = RuntimeException.class)
    public void testGetRolePlayerFail() throws Exception {
        Concept anotherConcept = mindmapsGraph.putEntityType("ac'");
        casting.addEdge((ConceptImpl) anotherConcept, DataType.EdgeLabel.ROLE_PLAYER);
        casting.getRolePlayer();
    }

    @Test
    public void testGetAssertion(){
        RoleTypeImpl role2 = (RoleTypeImpl) mindmapsGraph.putRoleType("Role 2");
        RelationTypeImpl genericRelation = (RelationTypeImpl) mindmapsGraph.putRelationType("gr").setSubject("Other Relation");
        RelationTypeImpl resourceType = (RelationTypeImpl) mindmapsGraph.putRelationType("rt").setSubject("A resource thing");
        RelationImpl relationValue = (RelationImpl) mindmapsGraph.putRelation(UUID.randomUUID().toString(), resourceType);

        relation.addEdge(genericRelation, DataType.EdgeLabel.ISA);
        relationValue.addEdge(resourceType, DataType.EdgeLabel.ISA);

        CastingImpl casting2 = mindmapsGraph.putCasting(role2, rolePlayer, relationValue);

        assertTrue(casting.getRelations().contains(relation));
        assertTrue(casting2.getRelations().contains(relationValue));
        assertThat(casting.getRelations().iterator().next(), instanceOf(Relation.class));
        assertThat(casting2.getRelations().iterator().next(), instanceOf(Relation.class));
    }

}