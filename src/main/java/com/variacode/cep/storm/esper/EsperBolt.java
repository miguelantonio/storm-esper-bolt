/*
 * Copyright 2015 Variacode
 *
 * Licensed under the GPLv2 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.gnu.org/licenses/old-licenses/gpl-2.0.txt
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.variacode.cep.storm.esper;

import com.espertech.esper.common.client.EPCompiled;
import com.espertech.esper.common.client.EventBean;
import com.espertech.esper.common.client.configuration.Configuration;
import com.espertech.esper.common.client.module.Module;
import com.espertech.esper.common.client.module.ModuleItem;
import com.espertech.esper.common.client.soda.EPStatementObjectModel;
import com.espertech.esper.common.client.util.EventTypeBusModifier;
import com.espertech.esper.compiler.client.CompilerArguments;
import com.espertech.esper.compiler.client.EPCompileException;
import com.espertech.esper.compiler.client.EPCompilerProvider;
import com.espertech.esper.runtime.client.EPDeployException;
import com.espertech.esper.runtime.client.EPDeployment;
import com.espertech.esper.runtime.client.EPRuntime;
import com.espertech.esper.runtime.client.EPRuntimeProvider;
import com.espertech.esper.runtime.client.EPStatement;
import com.espertech.esper.runtime.client.UpdateListener;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.apache.storm.generated.GlobalStreamId;
import org.apache.storm.generated.Grouping;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.FailedException;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.utils.Utils;

/**
 * Storm Bolt that embeds an Esper runtime instance for Complex Event Processing.
 */
public class EsperBolt extends BaseRichBolt implements UpdateListener {

    private static final long serialVersionUID = 1L;

    private OutputCollector collector;
    private transient EPRuntime epRuntime;
    private transient Tuple currentTuple;
    private Map<String, List<String>> outputTypes;
    private Map<String, Object> eventTypes;
    private Map<String, Map<String, Object>> streamEventTypes;
    private Set<String> statements;
    private Set<EPStatementObjectModel> objectStatements;

    public static void checkEPLSyntax(String statement) throws EsperBoltException {
        if (statement == null || statement.trim().isEmpty()) {
            throw new EsperBoltException("Statement cannot be null or empty");
        }
        try {
            EPCompilerProvider.getCompiler().eplToModel(statement, new Configuration());
        } catch (EPCompileException e) {
            throw new EsperBoltException(e.getMessage(), e);
        }
    }

    /**
     * Declares output streams and their field names using a Map.
     *
     * @param types map from output stream / event type name to ordered list of field names
     * @return this bolt instance
     */
    public EsperBolt addOutputTypes(Map<String, List<String>> types) {
        String error = "Output Types cannot be null";
        exceptionIfNull(error, types);
        exceptionIfCollectionHasNull(error, types.keySet());
        exceptionIfCollectionHasNull(error, types.values());
        Map<String, List<String>> copy = new LinkedHashMap<>();
        if (this.outputTypes != null) {
            copy.putAll(this.outputTypes);
        }
        for (Map.Entry<String, List<String>> entry : types.entrySet()) {
            List<String> fields = entry.getValue();
            exceptionIfCollectionHasNull(error, fields);
            copy.put(entry.getKey(), Collections.unmodifiableList(new ArrayList<>(fields)));
        }
        this.outputTypes = Collections.unmodifiableMap(copy);
        return this;
    }

    /**
     * Declares output streams and their field names from one or more POJO classes.
     * Each POJO's simple class name is used as the stream/event type name, and its
     * non-static, non-synthetic declared fields define the output fields.
     *
     * @param beans one or more POJO classes describing output event types
     * @return this bolt instance
     */
    public EsperBolt addOutputTypes(Class<?>... beans) {
        String error = "Output Types cannot be null";
        exceptionIfNull(error, beans);
        if (beans.length == 0) {
            throw new FailedException(error);
        }
        exceptionIfAnyNull(error, (Object[]) beans);
        return addOutputTypes(new LinkedHashSet<>(Arrays.asList(beans)));
    }

    /**
     * Declares output streams and their field names from a set of POJO classes.
     *
     * @param beans set of POJO classes describing output event types
     * @return this bolt instance
     */
    public EsperBolt addOutputTypes(Set<Class<?>> beans) {
        String error = "Output Types cannot be null";
        exceptionIfNull(error, beans);
        if (beans.isEmpty()) {
            throw new FailedException(error);
        }
        exceptionIfCollectionHasNull(error, beans);
        Map<String, List<String>> types = new LinkedHashMap<>();
        for (Class<?> bean : beans) {
            List<String> fields = new ArrayList<>();
            for (Field f : bean.getDeclaredFields()) {
                if (!Modifier.isStatic(f.getModifiers()) && !f.isSynthetic()) {
                    fields.add(f.getName());
                }
            }
            types.put(bean.getSimpleName(), fields);
        }
        return addOutputTypes(types);
    }

    public EsperBolt addEventTypes(Class<?> bean) {
        return addEventTypes(extractBeanEventTypes(bean));
    }

    /**
     * Declares input event property names and their types for a specific upstream component and stream.
     *
     * @param componentId upstream component ID
     * @param streamId upstream stream ID
     * @param bean POJO class describing the stream's event properties
     * @return this bolt instance
     */
    public EsperBolt addEventTypes(String componentId, String streamId, Class<?> bean) {
        return addEventTypes(componentId, streamId, extractBeanEventTypes(bean));
    }

    /**
     * Declares input event property names and their types for a specific upstream component (default stream).
     *
     * @param componentId upstream component ID
     * @param bean POJO class describing the component's event properties
     * @return this bolt instance
     */
    public EsperBolt addEventTypes(String componentId, Class<?> bean) {
        return addEventTypes(componentId, Utils.DEFAULT_STREAM_ID, extractBeanEventTypes(bean));
    }

    /**
     * Declares input event property names and their types for a specific upstream component and stream.
     *
     * @param componentId upstream component ID
     * @param streamId upstream stream ID
     * @param types map from field name to Java type
     * @return this bolt instance
     */
    public EsperBolt addEventTypes(String componentId, String streamId, Map<String, Object> types) {
        String error = "Event types cannot be null";
        exceptionIfAnyNull(error, componentId, streamId, types);
        exceptionIfCollectionHasNull(error, types.keySet());
        exceptionIfCollectionHasNull(error, types.values());
        Map<String, Map<String, Object>> copy = new LinkedHashMap<>();
        if (this.streamEventTypes != null) {
            copy.putAll(this.streamEventTypes);
        }
        copy.put(componentId + "_" + streamId, Collections.unmodifiableMap(new LinkedHashMap<>(types)));
        this.streamEventTypes = Collections.unmodifiableMap(copy);
        return this;
    }

    private Map<String, Object> extractBeanEventTypes(Class<?> bean) {
        String error = "Event types cannot be null";
        exceptionIfNull(error, bean);
        Map<String, Object> types = new LinkedHashMap<>();
        for (Field f : bean.getDeclaredFields()) {
            if (!Modifier.isStatic(f.getModifiers()) && !f.isSynthetic()) {
                types.put(f.getName(), f.getType());
            }
        }
        return types;
    }

    /**
     * Declares input event property names and their types.
     *
     * @param types map from field name to Java type
     * @return this bolt instance
     */
    public EsperBolt addEventTypes(Map<String, Object> types) {
        String error = "Event types cannot be null";
        exceptionIfNull(error, types);
        exceptionIfCollectionHasNull(error, types.keySet());
        exceptionIfCollectionHasNull(error, types.values());
        Map<String, Object> copy = new LinkedHashMap<>();
        if (this.eventTypes != null) {
            copy.putAll(this.eventTypes);
        }
        copy.putAll(types);
        this.eventTypes = Collections.unmodifiableMap(copy);
        return this;
    }

    /**
     * Adds EPL query statements to execute in the Esper runtime.
     *
     * @param statements set of EPL statement strings
     * @return this bolt instance
     */
    public EsperBolt addStatements(Set<String> statements) {
        String error = "Statements cannot be null";
        exceptionIfNull(error, statements);
        exceptionIfCollectionHasNull(error, statements);
        Set<String> copy = new LinkedHashSet<>();
        if (this.statements != null) {
            copy.addAll(this.statements);
        }
        copy.addAll(statements);
        this.statements = Collections.unmodifiableSet(copy);
        return this;
    }

    /**
     * Adds SODA object model statements to execute in the Esper runtime.
     *
     * @param objectStatements set of SODA statement models
     * @return this bolt instance
     */
    public EsperBolt addObjectStatements(Set<EPStatementObjectModel> objectStatements) {
        final String error = "Object Statements cannot be null";
        exceptionIfNull(error, objectStatements);
        exceptionIfCollectionHasNull(error, objectStatements);
        Set<EPStatementObjectModel> copy = new LinkedHashSet<>();
        if (this.objectStatements != null) {
            copy.addAll(this.objectStatements);
        }
        copy.addAll(objectStatements);
        this.objectStatements = Collections.unmodifiableSet(copy);
        return this;
    }

    /**
     * @param objectStatements set of SODA statement models
     * @return this bolt instance
     * @deprecated Use {@link #addObjectStatements(Set)} instead.
     */
    @Deprecated
    public EsperBolt addObjectStatemens(Set<EPStatementObjectModel> objectStatements) {
        return addObjectStatements(objectStatements);
    }

    private <O> O exceptionIfNull(String msg, O obj) {
        if (obj == null) {
            throw new FailedException(msg);
        }
        return obj;
    }

    private void exceptionIfAnyNull(String msg, Object... objs) {
        if (objs == null) {
            throw new FailedException(msg);
        }
        for (Object o : objs) {
            if (o == null) {
                throw new FailedException(msg);
            }
        }
    }

    private void exceptionIfCollectionHasNull(String msg, Collection<?> collection) {
        if (collection == null) {
            throw new FailedException(msg);
        }
        for (Object o : collection) {
            if (o == null) {
                throw new FailedException(msg);
            }
        }
    }

    /**
     * {@inheritDoc}
     *
     * @param ofd output fields declarer
     */
    @Override
    public void declareOutputFields(OutputFieldsDeclarer ofd) {
        if (this.outputTypes == null) {
            throw new FailedException("outputTypes cannot be null");
        }
        for (Map.Entry<String, List<String>> outputEventType : this.outputTypes.entrySet()) {
            List<String> fields = outputEventType.getValue();
            if (fields == null) {
                throw new FailedException("Output fields cannot be null");
            }
            ofd.declareStream(outputEventType.getKey(), new Fields(new ArrayList<>(fields)));
        }
    }

    /**
     * {@inheritDoc}
     *
     * @param map storm configuration map
     * @param tc topology context
     * @param oc output collector
     */
    @Override
    public void prepare(Map<String, Object> map, TopologyContext tc, OutputCollector oc) {
        this.collector = oc;
        if ((this.eventTypes == null && this.streamEventTypes == null)
                || (this.objectStatements == null && this.statements == null)) {
            throw new FailedException("Event types cannot be null and at least one type of statement has to be not null");
        }
        Configuration cepConfig = new Configuration();
        cepConfig.getCompiler().getByteCode().setAccessModifiersPublic();
        cepConfig.getCompiler().getByteCode().setBusModifierEventType(EventTypeBusModifier.BUS);
        for (Map.Entry<GlobalStreamId, Grouping> a : tc.getThisSources().entrySet()) {
            String streamKey = a.getKey().get_componentId() + "_" + a.getKey().get_streamId();
            Map<String, Object> targetEventTypes = (this.streamEventTypes != null && this.streamEventTypes.containsKey(streamKey))
                    ? this.streamEventTypes.get(streamKey)
                    : this.eventTypes;
            Fields f = tc.getComponentOutputFields(a.getKey());
            if (targetEventTypes == null || !targetEventTypes.keySet().containsAll(f.toList())) {
                throw new FailedException("Event types and fields from source streams do not match: Event Types="
                        + (targetEventTypes == null ? "null" : Arrays.toString(targetEventTypes.keySet().toArray()))
                        + " Stream Fields=" + Arrays.toString(f.toList().toArray()));
            }
            cepConfig.getCommon().addEventType(streamKey, new LinkedHashMap<>(targetEventTypes));
        }
        String runtimeUri = tc.getThisComponentId() + "_" + tc.getThisTaskId() + "_" + UUID.randomUUID();
        this.epRuntime = EPRuntimeProvider.getRuntime(runtimeUri, cepConfig);
        if (!processStatements(cepConfig)) {
            throw new FailedException("At least one type of statement has to be not empty");
        }
    }

    private boolean processStatements(Configuration cepConfig) {
        boolean hasStatements = false;
        if (this.statements != null) {
            for (String s : this.statements) {
                deployStatement(s, cepConfig);
                hasStatements = true;
            }
        }
        if (this.objectStatements != null) {
            for (EPStatementObjectModel s : this.objectStatements) {
                deployObjectStatement(s, cepConfig);
                hasStatements = true;
            }
        }
        return hasStatements;
    }

    private void deployStatement(String epl, Configuration cepConfig) {
        try {
            CompilerArguments args = new CompilerArguments(cepConfig);
            args.getPath().add(this.epRuntime.getRuntimePath());
            EPCompiled compiled = EPCompilerProvider.getCompiler().compile(epl, args);
            EPDeployment deployment = this.epRuntime.getDeploymentService().deploy(compiled);
            for (EPStatement stmt : deployment.getStatements()) {
                stmt.addListener(this);
            }
        } catch (EPCompileException | EPDeployException e) {
            throw new FailedException("Failed to compile or deploy EPL statement: " + e.getMessage(), e);
        }
    }

    private void deployObjectStatement(EPStatementObjectModel model, Configuration cepConfig) {
        try {
            CompilerArguments args = new CompilerArguments(cepConfig);
            args.getPath().add(this.epRuntime.getRuntimePath());
            Module module = new Module();
            module.getItems().add(new ModuleItem(model));
            EPCompiled compiled = EPCompilerProvider.getCompiler().compile(module, args);
            EPDeployment deployment = this.epRuntime.getDeploymentService().deploy(compiled);
            for (EPStatement stmt : deployment.getStatements()) {
                stmt.addListener(this);
            }
        } catch (EPCompileException | EPDeployException e) {
            throw new FailedException("Failed to compile or deploy SODA statement: " + e.getMessage(), e);
        }
    }

    /**
     * {@inheritDoc}
     *
     * @param tuple incoming Storm tuple
     */
    @Override
    public void execute(Tuple tuple) {
        this.currentTuple = tuple;
        try {
            Map<String, Object> tuplesper = new HashMap<>();
            for (String f : tuple.getFields()) {
                tuplesper.put(f, tuple.getValueByField(f));
            }
            this.epRuntime.getEventService().sendEventMap(
                    tuplesper,
                    tuple.getSourceComponent() + "_" + tuple.getSourceStreamId());
            collector.ack(tuple);
        } catch (Exception e) {
            if (collector != null) {
                collector.reportError(e);
                collector.fail(tuple);
            } else {
                throw new FailedException(e);
            }
        } finally {
            this.currentTuple = null;
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void cleanup() {
        if (this.epRuntime != null) {
            this.epRuntime.destroy();
        }
    }

    /**
     * {@inheritDoc}
     *
     * @param newEvents newly matching events
     * @param oldEvents expired events
     * @param statement the statement producing the update
     * @param runtime the Esper runtime
     */
    @Override
    public void update(EventBean[] newEvents, EventBean[] oldEvents, EPStatement statement, EPRuntime runtime) {
        if (newEvents == null) {
            return;
        }
        try {
            for (EventBean newEvent : newEvents) {
                List<Object> tuple = new ArrayList<>();
                String eventTypeName = newEvent.getEventType().getName();
                String targetStream;
                if (outputTypes != null && outputTypes.containsKey(eventTypeName)) {
                    targetStream = eventTypeName;
                } else if (outputTypes != null && outputTypes.containsKey("default")) {
                    targetStream = "default";
                } else {
                    throw new EsperBoltException(
                            "Received event for undeclared output stream '" + eventTypeName + "'");
                }
                for (String field : outputTypes.get(targetStream)) {
                    tuple.add(newEvent.get(field));
                }
                if (this.currentTuple != null) {
                    collector.emit(targetStream, this.currentTuple, tuple);
                } else {
                    collector.emit(targetStream, tuple);
                }
            }
        } catch (Exception e) {
            if (collector != null) {
                collector.reportError(e);
            }
        }
    }

}
