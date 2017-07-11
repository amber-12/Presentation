
public class Employee implements Comparable<Employee> {
int id,age;
Integer salary;
String name;

public Employee(int id,int age,int salary,String name)
{
	this.id=id;
	this.age=age;
	this.name=name;
	this.salary=salary;
}

@Override
public int compareTo(Employee o) {
	// TODO Auto-generated method stub

	
	return this.id-o.id;
}


private Integer getSalary() {
	// TODO Auto-generated method stub
	return salary;
}

public String toString()
{
return "{id :"+id+",age :"+age+",salary :"+salary+",name :"+name+"}";
}

}
