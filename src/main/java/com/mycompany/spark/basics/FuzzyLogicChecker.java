/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.mycompany.spark.basics;

import net.sourceforge.jFuzzyLogic.FIS;
import net.sourceforge.jFuzzyLogic.rule.FuzzyRuleSet;

/**
 *
 * @author workspace
 */
public class FuzzyLogicChecker {
    public static void main(String args[]){
        // Load from 'FCL' file
        String fileName = "/home/workspace/Desktop/jFuzzyLogic_v3.0/fcl/health.fcl";
        FIS fis = FIS.load(fileName,true);
        // Error while loading?
        if( fis == null ) { 
            System.err.println("Can't load file: '" 
                                   + fileName + "'");
            return;
        }

        // Show 
        fis.chart();

        // Set inputs
        fis.setVariable("age", 10);
        fis.setVariable("systolic_bp", 200);
        fis.setVariable("diastolic_bp", 150);
        fis.setVariable("gender", 1);
        fis.setVariable("cholestrol", 2);
        fis.setVariable("smoking", 1);

        // Evaluate
        fis.evaluate();

        // Show output variable's chart 
        fis.getVariable("risk").chartDefuzzifier(true);

        // Print ruleSet
        System.out.println(fis);
        
        System.out.println(fis.getVariable("risk").getLatestDefuzzifiedValue());
    }
}
