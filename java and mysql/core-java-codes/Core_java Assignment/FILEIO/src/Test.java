import java.io.Serializable;


public class Test implements Serializable{
	
	String name;
	int age;
	long phone;
	String address;
	
	public Test(String name,int age,long phone,String address)
	{
		this.address=address;
		this.age=age;
		this.name=name;
		this.phone=phone;
	}

	

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public int getAge() {
		return age;
	}

	public void setAge(int age) {
		this.age = age;
	}

	public long getPhone() {
		return phone;
	}

	public void setPhone(long phone) {
		this.phone = phone;
	}

	public String getAddress() {
		return address;
	}

	public void setAddress(String address) {
		this.address = address;
	}



	@Override
	public String toString() {
		return "Test [name=" + name + ", age=" + age + ", phone=" + phone
				+ ", address=" + address + "]";
	}
	
}
