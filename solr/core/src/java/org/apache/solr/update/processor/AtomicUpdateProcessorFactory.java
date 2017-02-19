/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.solr.update.processor;

import static org.apache.solr.common.SolrException.ErrorCode.SERVER_ERROR;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.HashMap;
import java.util.Map;

import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.update.AddUpdateCommand;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An update processor that will convert conventional field-value document to atomic update document
 * on the basis of init params specified in its definition in solrconfig.xml
 *
 * <pre class="prettyprint">
 *  &lt;processor class="solr.AtomicUpdateProcessorFactory"&gt;
 *    &lt;str name="fieldName"&gt;atomic-action&lt;/str&gt;
 *  &lt;/processor&gt;
 * </pre>
 * 
 * currently supports all types of atomic updates
 */

public class AtomicUpdateProcessorFactory extends UpdateRequestProcessorFactory{

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  protected IndexSchema schema;
  
  private final String ADD="add";
  private final String INC="inc";
  private final String REMOVE="remove";
  private final String SET="set";
  private final String REMOVEREGEX="removeregex";
  private String uniq_key_doc;
  
  // contains key-value pairs of fieldName and the atomic operation to be applied
  private Map<String,String> atomicOpsFieldMap;

  @SuppressWarnings({"static-access", "rawtypes", "null"})
  @Override
  public void init(final NamedList args) {
    if (args != null && 0 < args.size()) { 
      atomicOpsFieldMap = SolrParams.toSolrParams(args).toMap(args);
      Object flag = null;
      if((flag = verifyInitParams()).getClass().isInstance(String.class))
        throw new SolrException(SERVER_ERROR,
            "Unexpected init param(s): '" +
                flag + "'");
    }
    else
      if (args == null || 0 >= args.size()) 
        throw new SolrException(SERVER_ERROR,
            "No init param(s) passed: '" +
                this.getClass().getName() + "'");
  }
  
  public Map<String,String> getAtomicOpsFieldMap(){
    return atomicOpsFieldMap;
  }
  
  private Object verifyInitParams(){
    for(String fieldName : atomicOpsFieldMap.keySet()){
      if(fieldName == null || fieldName.trim().length()==0)
        return "empty fieldName passed";
    }
    for(String op : atomicOpsFieldMap.values())
      if(!op.equals(ADD) || !op.equals(INC) || !op.equals(REMOVE) || 
          !op.equals(SET) || !op.equals(REMOVEREGEX))
        return "invalid atomic action '"+op+"'";
    return true;
  }

  @Override
  public UpdateRequestProcessor getInstance(SolrQueryRequest req, SolrQueryResponse rsp, 
      UpdateRequestProcessor next) {
    return new AtomicUpdateProcessor(req, atomicOpsFieldMap, next);
  }
  
  public class AtomicUpdateProcessor extends UpdateRequestProcessor{
    
    @SuppressWarnings("unused")
    private final SolrQueryRequest req;
    private final Map<String,String> atomicOpsFieldMap;
    private final UpdateRequestProcessor next;
    
    public AtomicUpdateProcessor(SolrQueryRequest req, Map<String,String> atomicOpsFieldMap,
        UpdateRequestProcessor next) {
      super(next);
      schema = req.getSchema();
      for(String fieldName: atomicOpsFieldMap.keySet()){
        if(!schema.isDynamicField(fieldName) && !schema.getFields().keySet().contains(fieldName)){
          throw new SolrException(SERVER_ERROR,
              "Field specified in AtomicUpdateProcessor definition not available in schema: '" +
                  fieldName  + "'");
        }
      }
      uniq_key_doc = schema.getUniqueKeyField().getName();
      this.req=req;
      this.atomicOpsFieldMap = atomicOpsFieldMap;
      this.next = next;
    }
    
    /*
     * 1. convert incoming update document to atomic-type update document 
     * for specified fields in processor definition.
     * 2. if incoming update document contains already atomic-type updates,
     * the operation specified for field in processor definition must and must match
     * with atomic-operation in incoming doc field, else FORBIDDEN
     * 3. fields not specified in processor definition in solrconfig.xml for atomic action
     * will be treated as conventional updates.
     * 
     */
    @SuppressWarnings("unchecked")
    @Override
    public void processAdd(AddUpdateCommand cmd)
        throws IOException {
      SolrInputDocument orgdoc = cmd.getSolrInputDocument();
      if(orgdoc.get(uniq_key_doc)==null)
        throw new SolrException(ErrorCode.BAD_REQUEST,
            "Document passed with no unique field: '"+uniq_key_doc+"'"); 
      
      for (Map.Entry<String, String> field : atomicOpsFieldMap.entrySet()){
        
        if(orgdoc.get(field.getKey())!=null ){
          boolean alreadyAtomicUpdate = false;
          
          /* in case of multiple threads executing this URP simultaneously, 
           * if the first thread changes the incoming doc to atomic-type (for one or more fields)
           * the second thread will by-pass/skip the modification if the atomic-operation matches
           * with already present atomic-update, otherwise BAD_REQUEST/FORBIDDEN
           */
          if(orgdoc.get(field.getKey()).getValue() instanceof Map){          
            alreadyAtomicUpdate = true;
            Map<Object,Object> alreadyPresentAtomicValue = (Map<Object,Object>) orgdoc.get(field.getKey()).getValue();
           
            String errormsg = "Cannot apply '"+field.getValue()+"' atomic operation on field: "
                + "'"+field.getKey()+"' for doc: '"+orgdoc.getField(uniq_key_doc)+"'";
            
            // if the incoming doc has incomplete/empty/null atomic updates, BAD_REQUEST
            if(alreadyPresentAtomicValue.size()==0 || alreadyPresentAtomicValue.keySet()==null || 
                alreadyPresentAtomicValue.keySet().size()==0)
              throw new SolrException(ErrorCode.BAD_REQUEST,errormsg);
            
            // if the incoming doc has atomic update operation, 
            // different from what specified in processor definition
            if(!field.getValue().equals(alreadyPresentAtomicValue.keySet().toArray()[0]))
              throw new SolrException(ErrorCode.FORBIDDEN,
                  errormsg+","+ "incoming doc contains different atomic-op update: '"
                          +orgdoc.get(field.getKey()).getValue()+"'");
          }
          
          /* if the incoming field value is already atomic and 
           *  matches the operation getting applied below skip it then
           *  else update the field, make it atomic
          */
          if(!alreadyAtomicUpdate){
            Map<Object,Object> newFieldValue = new HashMap<Object,Object>();
            switch(field.getValue()){
              case ADD:
                newFieldValue.put(ADD, orgdoc.get(field.getKey()).getValue());
                break;
              case REMOVE:
                newFieldValue.put(REMOVE, orgdoc.get(field.getKey()).getValue());
                break;
              case SET:
                newFieldValue.put(SET, orgdoc.get(field.getKey()).getValue());
                break;
              case INC:
                newFieldValue.put(INC, orgdoc.get(field.getKey()).getValue());
                break;
              case REMOVEREGEX:
                newFieldValue.put(REMOVEREGEX,orgdoc.get(field.getKey()).getValue());
                break;
              default:
                //This case will never happen, still there!
                break;
            }
            //setting atomic-type (map) value for the specified field
            orgdoc.setField(field.getKey(), newFieldValue);
          }
        }
      }
      if (next != null)
        next.processAdd(cmd);
      else super.processAdd(cmd);
    }
  }
}