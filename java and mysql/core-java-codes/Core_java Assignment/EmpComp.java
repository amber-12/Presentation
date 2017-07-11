import java.util.Comparator;


public class EmpComp implements Comparator<Employee> {

	
	@Override
	public int compare(Employee e1, Employee e2) {
		// TODO Auto-generated method stub
		if(e1.salary>e2.salary)
		{
			return 1;	
		}
		
		else if(e1.salary<e2.salary)
		{
			return -1;
		}
		else
		{
			return 0;
			
		}
		
		
		
	}
	

}
