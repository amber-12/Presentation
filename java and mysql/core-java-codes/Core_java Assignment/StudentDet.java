import java.io.Serializable;
import java.util.Scanner;


public class StudentDet implements Serializable {
	int id,age;
	long phone;
	String name;
	Scanner sc=new Scanner(System.in);
	@Override
	public String toString() {
		return "StudentDet [id=" + id + ", age=" + age + ", phone=" + phone
				+ ", name=" + name + "]";
	}

	public int getAge() {
		return age;
	}

	public void setAge(int age) {
		this.age = age;
	}
	

	public int getId() {
		return id;
	}

	public void setId(int id) {
		this.id = id;
	}

	public long getPhone() {
		return phone;
	}

	public void setPhone(long phone) {
		this.phone = phone;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String accept()
	{
		System.out.println("enter id");
		id=sc.nextInt();
		System.out.println("enter age");
		age=sc.nextInt();
		System.out.println("phone");
		phone=sc.nextLong();
		System.out.println("enter name");
	     name=sc.next();
		return id+" "+age+" "+phone+" "+name;
			
	}
	
	
	
}
