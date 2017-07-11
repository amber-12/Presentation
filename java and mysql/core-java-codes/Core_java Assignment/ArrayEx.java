import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;

public class ArrayEx {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		ArrayList<Employee> obj = new ArrayList<Employee>();

		obj.add(new Employee(22, 24, 50000, "Amber"));
		obj.add(new Employee(1, 24, 30000, "rahul"));
		obj.add(new Employee(2, 23, 60000, "gaurav"));
		obj.add(new Employee(6, 23, 20000, "anil"));
		obj.add(new Employee(3, 23, 7000, "akshay"));
		obj.add(new Employee(4, 23, 6000, "nitesh"));
		System.out.println("------------------------------------------------------------------------");
		System.out.println("------------------------------------------------------------------------");
		System.out.println("list of student before sorting");
		System.out.println(obj);
		System.out.println("------------------------------------------------------------------------");
		System.out.println("------------------------------------------------------------------------");
		Collections.sort(obj);
		System.out.println("list of student after sorting");
		System.out.println(obj);
		System.out.println("-----------------------------------------");
		System.out.println("------------------------------------------------------------------------");
		Employee max = Collections.max(obj, new EmpComp());
		System.out.println("heighest salary of employee details" + max);
		System.out.println("-------------------------------------------------------");
		System.out.println("------------------------------------------------------------------------");
		Collections.sort(obj, new EmpComp());

		System.out.println("list of employee order by salary in assending ");
		System.out.println(obj);
	}

}
