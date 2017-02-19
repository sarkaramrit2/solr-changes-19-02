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

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.solr.common.SolrException;
import org.apache.solr.common.util.NamedList;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * test class for @see AtomicUpdateProcessorFactory
 */

public class AtomicUpdateProcessorFactoryTest extends UpdateProcessorTestBase{

  private AtomicUpdateProcessorFactory factoryToTest = new AtomicUpdateProcessorFactory();
  @SuppressWarnings("rawtypes")
  private NamedList args = new NamedList<String>();
  
  @BeforeClass
  public static void beforeClass() throws Exception {
    initCore("solrconfig-atomic-update-processor.xml", "schema.xml");
  }
  
  @SuppressWarnings("unchecked")
  @Before
  public void initArgs() {
    args.add("title_s", "add");
    args.add("genre_s", "set");
  }
  
  // test for atomicFieldMap getting initiated
  @Test
  public void testFullInit() {
    factoryToTest.init(args);
    Object[] inputFieldNames = factoryToTest.getAtomicOpsFieldMap().keySet().toArray();
    assertEquals("title_s", (String)inputFieldNames[0]);
    assertEquals("genre_s", (String)inputFieldNames[1]);
    Object[] atomicActions = factoryToTest.getAtomicOpsFieldMap().values().toArray();
    assertEquals("add", (String)atomicActions[0]);
    assertEquals("set", (String)atomicActions[1]);
  }
  
  // test for empty init parameters passed along in processor definition in solrconfig.xml, throw SolrException
  @Test
  public void testInitEmpty() {
    args.clear();
    try {
      factoryToTest.init(args);
    } catch (SolrException e) {
      assertEquals("No init param(s) passed: 'org.apache.solr.update.processor.AtomicUpdateProcessorFactory'", e.getMessage());
    }
  }
  
  // test for empty or null fieldName (key) passed in processor definition in solrconfig.xml, throw SolrException
  @SuppressWarnings("unchecked")
  @Test
  public void testInitEmptyFieldName() {
    args.add(null, "add");
    try {
      factoryToTest.init(args);
    } catch (SolrException e) {
      assertEquals("Unexpected init param(s): empty fieldName passed", e.getMessage());
    }
    args.clear();
    args.add("", "add");
    try {
      factoryToTest.init(args);
    } catch (SolrException e) {
      assertEquals("Unexpected init param(s): empty fieldName passed", e.getMessage());
    }
  }
  
  // test for invalid atomic operation passed in processor definition in solrconfig.xml, throw SolrException
  @SuppressWarnings("unchecked")
  @Test
  public void testInitInvalidAtomicOp() {
    args.add("fieldA", "delete");
    try {
      factoryToTest.init(args);
    } catch (SolrException e) {
      assertEquals("Unexpected init param(s): invalid atomic action 'delete'", e.getMessage());
    }
  }
  
  // test for fieldName not available as a valid field (dynamic or conventional) in schema.xml, throw SolrException
  @Test
  public void testFieldsNotPresentInSchema(){
    try {
      processAdd("basic-1-atomic-processor",
          doc(f("id", "1")));
    } catch (SolrException e) {
      assertEquals("Field specified in AtomicUpdateProcessor definition not available in schema: 'random'", e.getMessage());
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
  
//test for incoming doc contains already atomic-type document
 @Test
 public void testIncomingFieldValueAlreadyAtomic(){
   try {
     Map<Object,Object> catValues = new HashMap<Object,Object>();
     processAdd("basic-0-atomic-processor",
         doc(f("id", "1"),
             f("cat", catValues)));
   } catch (SolrException e) {
     assertEquals("Cannot apply 'add' atomic operation on field: 'cat' for doc: 'id=1'", e.getMessage());
   } catch (IOException e) {
     throw new RuntimeException(e);
   }
   try {
     Map<Object,Object> catValues = new HashMap<Object,Object>();
     catValues.put("set", "tendulkar");
     processAdd("basic-0-atomic-processor",
         doc(f("id", "1"),
             f("cat", catValues)));
   } catch (SolrException e) {
     assertEquals("Cannot apply 'add' atomic operation on field: 'cat' for doc: 'id=1',incoming doc contains different atomic-op update: '{set=tendulkar}'", e.getMessage());
   } catch (IOException e) {
     throw new RuntimeException(e);
   }
 }
  
  /* basic level tests for 'add' 'set' 'remove' 'removeregex' 'inc' atomic operations.
   * conversion of conventional update document to atomic update document are verified 
   * detailed tests on atomic updates are present in AtomicUpdatesTest
   * @see AtomicUpdatesTest
   * 
   * 'cat'- mvf string field
   * 'subject' - single-valued string field
   * 'sindsto' - single-valued string field
   * 'bindsto' - single-valued boolean field
   * 'count_i' - single-valued dynamic int field
   */
  @Test
  public void testBasicOps() throws IOException {

    // insert document in conventional manner
    processAdd("basic-processor",
        doc(f("id", "1"),
            f("cat", "sachin"),
            f("subject", "rahul"),
            f("sindsto", "virat"),
            f("bindsto", false),
            f("count_i", 13)));
    
    processCommit("basic-processor");

    assertQ("Check the total number of docs",
        req("q","*:*")
        , "//result[@numFound=1]");
    
    assertQ("Check the total number of docs",
        req("q","id:1")
        , "//result[@numFound=1]");

    // update document via AtomicUpdateProcessor (ops - add, set, remove, inc)
    processAdd("basic-0-atomic-processor",
        doc(f("id", "1"),
            f("cat", "tendulkar"),
            f("subject", "dravid"),
            f("sindsto", "virat"),
            f("bindsto", true),
            f("count_i", 5)
            ));
    
    processCommit("basic-0-atomic-processor");
           
    // subject field updated from 'rahul' to 'dravid'
    assertQ("check the total number of docs",
        req("q","subject:dravid")
        , "//result[@numFound=1]");
    
    assertQ("check the total number of docs",
        req("q","subject:rahul")
        , "//result[@numFound=0]");
    
    // sindsto field removed from the document
    assertQ("check the total number of docs",
        req("q","sindsto:virat")
        , "//result[@numFound=0]");
    
    assertQ("check the total number of docs",
        req("q","cat:sachin")
        , "//result[@numFound=1]");
    
    // cat field now contains 'sachin' and 'tendulkar'
    assertQ("check the total number of docs",
        req("q","cat:tendulkar")
        , "//result[@numFound=1]");
    
    // count_i field increased by 5 to 18
    assertQ("check the total number of docs",
        req("q","count_i:18")
        , "//result[@numFound=1]");
    
    /* bindsto field updated in conventional manner, 
    *  no atomic operation defined in processor definition 
    */
    assertQ("check the total number of docs",
        req("q","bindsto:true")
        , "//result[@numFound=1]");
    
    assertQ("check the total number of docs",
        req("q","bindsto:false")
        , "//result[@numFound=0]");
    
    // update document via AtomicUpdateProcessor (ops - removeregex)
    processAdd("basic-2-atomic-processor",
        doc(f("id", "1"),
            f("cat", ".endulka.")));
    
    processCommit("basic-2-atomic-processor");
    
    assertQ("check the total number of docs",
        req("q","id:1")
        , "//result[@numFound=1]");
    
    assertQ("check the total number of docs",
        req("q","cat:sachin")
        , "//result[@numFound=1]");
    
    // regex which matched 'tendulkar' with cat field values removed
    assertQ("check the total number of docs",
        req("q","cat:tendulkar")
        , "//result[@numFound=0]");
    
    Map<Object,Object> atomicValue = new HashMap<>();
    atomicValue.put("inc", 10);
  
    // incoming doc with atomic-field value pushed through URP
    processAdd("basic-0-atomic-processor",
        doc(f("id", "1"),
            f("count_i", atomicValue)
            ));
    
    processCommit("basic-0-atomic-processor");

    // should be allowed to pass and perform the atomic update
    assertQ("check the total number of docs",
        req("q","count_i:28")
        , "//result[@numFound=1]");
    
  }
  
}