package ai.grakn.graql.internal.gremlin;

import ai.grakn.GraknGraph;
import ai.grakn.concept.TypeLabel;
import ai.grakn.graql.Var;
import ai.grakn.graql.VarName;
import ai.grakn.graql.admin.Answer;
import ai.grakn.graql.admin.Conjunction;
import ai.grakn.graql.admin.Disjunction;
import ai.grakn.graql.admin.PatternAdmin;
import ai.grakn.graql.admin.RelationPlayer;
import ai.grakn.graql.admin.VarAdmin;
import ai.grakn.graql.internal.pattern.property.IsaProperty;
import ai.grakn.graql.internal.pattern.property.RelationProperty;
import ai.grakn.graql.internal.query.match.MatchQueryBase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static ai.grakn.graql.Graql.and;
import static ai.grakn.graql.Graql.match;
import static ai.grakn.graql.internal.pattern.Patterns.var;

/**
 *
 */
public class TypeOptimisation {

    protected static final Logger LOG = LoggerFactory.getLogger(MatchQueryBase.class);

    public static PatternAdmin generateOntologyQueryAndMap(Conjunction<PatternAdmin> initialPatterns, GraknGraph graph) {
        Set<Var> ontologyQuery = new HashSet<>();
        Set<VarName> knownTypes = new HashSet<>();
        Set<VarName> unknownRoles = new HashSet<>();
        // take the pattern and break into parts that can be executed independently
        //TODO: I think we can assume this is done in graql previous to the optimiser being called
        Disjunction<Conjunction<VarAdmin>> initialNormalForm = initialPatterns.getDisjunctiveNormalForm();
        // go through independent queries looking for types
        initialNormalForm.getPatterns().iterator().forEachRemaining(aVarAdmin -> {
            // for each query obtain ALL the vars in a list and consider in turn
            // TODO: absorb this call into the disjunctive normal form method
            aVarAdmin.getVars().iterator().forEachRemaining(aVar -> {
                // get type if it has been specified and is not a var itself
                getTypeVar(aVar, knownTypes).ifPresent((typeVar) -> {
                    if (typeVar.getTypeLabel().isPresent()) {
                        ontologyQuery.add(typeVar);
                    }
                });
                // if there is a relation property attached to the var it is a relation - current starting point
                Optional<RelationProperty> relation = aVar.getProperty(RelationProperty.class);
                if (relation.isPresent()) {
                    // get a var representing the relation type
                    VarAdmin relationTypeVar = getTypeVarOrDummy(aVar, knownTypes);
                    // cycle through role players to construct ontology query
                    relation.get().getRelationPlayers().forEach(
                            relationPlayer -> {
                                // get role type
                                VarAdmin roleTypeVar = getRoleTypeVar(relationPlayer, knownTypes, unknownRoles);
                                // get roleplayer type
                                VarAdmin rolePlayerType = getTypeVarOrDummy(relationPlayer.getRolePlayer(), knownTypes);
                                // put together an ontology query
                                rolePlayerType.plays(roleTypeVar);
                                relationTypeVar.relates(roleTypeVar);
                                ontologyQuery.add(rolePlayerType);
                            }
                    );
                    ontologyQuery.add(relationTypeVar);
                }
            });
        });

        // execute the ontology query if there is anything to infer
        //TODO: move this inside the disjunction loop
        if (!ontologyQuery.isEmpty()) {
            LOG.trace("the initial query is: " + initialPatterns.toString());
            LOG.trace("known types:");
            knownTypes.stream().map(VarName::toString).collect(Collectors.toSet()).forEach(LOG::trace);
            LOG.trace("the ontology query is: " + match(ontologyQuery).toString());
            List<Answer> results = match(ontologyQuery).withGraph(graph).execute();

            // bundle results into sets to deduplicate
            Map<VarName, Set<TypeLabel>> deduplicatedResults = new HashMap<>();
            results.forEach(result -> {
                result.forEach((varName, concept) -> {
                    if (!knownTypes.contains(varName)) {
                        deduplicatedResults.computeIfAbsent(varName, (x) -> new HashSet<>()).add(concept.asType().getLabel());
                    }
                });
            });

            LOG.trace("found " + String.valueOf(deduplicatedResults.size()) + " vars without types.");
            LOG.trace("options for these types are:");
            deduplicatedResults.forEach((k, v) -> LOG.trace(k.toString() + ' ' + v.toString()));

            // append types to the initial query
            Set<Var> queryExtension = deduplicatedResults.keySet().stream()
                    .filter((x) -> deduplicatedResults.get(x).size() == 1)
                    .filter((x) -> !knownTypes.contains(x))
                    .map((x) -> {
                        if (unknownRoles.contains(x)) {
                            return var().label(deduplicatedResults.get(x).iterator().next()).sub(var(x));
                        } else {
                            return var(x).isa(deduplicatedResults.get(x).iterator().next().toString());
                        }
                    })
                    .collect(Collectors.toSet());

            PatternAdmin extendedPatterns = and(and(initialPatterns), and(queryExtension)).admin();
            LOG.trace("the extended query is: "+extendedPatterns.toString());
            return extendedPatterns;
        } else {
            return initialPatterns;
        }
    }

    private static Optional<VarAdmin> getTypeVar(VarAdmin aVar, Set<VarName> knownTypes) {
        Optional<IsaProperty> property = aVar.getProperty(IsaProperty.class);
        if (property.isPresent()) {
            VarName varName = aVar.getVarName();
            Optional<TypeLabel> mightBeATypeName = property.get().getType().getTypeLabel();
            // check if the type is known - if not just return
            if (mightBeATypeName.isPresent()) {
                knownTypes.add(varName);
                return Optional.of(var(varName).label(mightBeATypeName.get()).admin());
            } else {
                return Optional.of(var(varName).admin());
            }
        } else {
            return Optional.empty();
        }
    }

    private static VarAdmin getTypeVarOrDummy(VarAdmin aVar, Set<VarName> knownTypes) {
        Optional<VarAdmin> mightBeVar = getTypeVar(aVar, knownTypes);
        if (mightBeVar.isPresent()) {
            return mightBeVar.get();
        } else {
            return var(aVar.getVarName()).admin();
        }
    }

    private static VarAdmin getRoleTypeVar(RelationPlayer relationPlayer, Set<VarName> knownTypes, Set<VarName> unknownRoles) {
        Optional<VarAdmin> aRole = relationPlayer.getRoleType();
        if (aRole.isPresent()) {
            VarAdmin roleTypeVar = aRole.get();
            Optional<TypeLabel> mightBeVar = roleTypeVar.getTypeLabel();
            if (mightBeVar.isPresent()) {
                knownTypes.add(roleTypeVar.getVarName());
            } else {
                unknownRoles.add(roleTypeVar.getVarName());
            }
            return roleTypeVar;
        } else {
            throw new RuntimeException("role types must ALL be specified");
        }
    }
}
