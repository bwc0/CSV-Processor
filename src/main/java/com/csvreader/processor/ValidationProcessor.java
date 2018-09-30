package com.csvreader.processor;

import com.csvreader.model.Employee;
import org.springframework.batch.item.ItemProcessor;

public class ValidationProcessor  implements ItemProcessor<Employee, Employee> {
    /*
        Validate if id is set
        if id is parsable integer
        if id is positive integer
        if null return null, indicating record wasn't processed
        if succeeds then return employee object
     */

    @Override
    public Employee process(Employee employee) throws Exception {
        if(employee.getId() == null) {
            System.out.println("Missing employee id: " + employee.getId());
            return null;
        }

        try {
            if (Integer.valueOf(employee.getId()) <= 0) {
                System.out.println("Invalid employee id: " + employee.getId());
                return null;
            }
        } catch (NumberFormatException nfe) {
            System.out.println("Invalid employee id: " + employee.getId());
            return  null;
        }

        return employee;
    }
}
