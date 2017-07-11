import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Date;


public class ObjectInputEx {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		
		try
		{
			FileOutputStream fobj=new FileOutputStream("F:\\Objectdata.txt");
			ObjectOutputStream oos=new ObjectOutputStream(fobj);
			oos.writeObject(new Date());
			Test obj=new Test("amber",22,220406,"Abc chouck pune");
			Test ob1=new Test("akshay",24,998877,"nigdi pune");
			Test ob2=new Test("rahul",23,223366,"akurdi pune");
			oos.writeObject(obj);
			oos.writeObject(ob1);
			oos.writeObject(ob2);
			System.out.println("employee details stored sucessfully");
			
			FileInputStream fis=new FileInputStream("F:\\objectdata.txt");
			ObjectInputStream ois=new ObjectInputStream(fis);
			
			Date dd=(Date)ois.readObject();
			System.out.println(dd);
			Object ob=null;
			while((ob=ois.readObject())!=null)
			{
				if(ob instanceof Test)
				{
					System.out.println(((Test)ob).toString());
				}
			}
			
			/*Test emp;
			emp=(Test)ois.readObject();
			
			System.out.println(emp.getAddress()+" "+emp.age+" "+emp.name+" "+emp.getPhone());
			emp=(Test)ois.readObject();
			System.out.println(emp.getAddress()+" "+emp.age+" "+emp.name+" "+emp.getPhone());
			emp=(Test)ois.readObject();
			System.out.println(emp.getAddress()+" "+emp.age+" "+emp.name+" "+emp.getPhone());
			*/
			
		}  
		
     catch(Exception e)
		{
    	 System.out.println(e.getMessage());
		}
	}

}
