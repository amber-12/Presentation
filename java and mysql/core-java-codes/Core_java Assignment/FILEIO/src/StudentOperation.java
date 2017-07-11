import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Scanner;


public class StudentOperation {
	public static ArrayList<StudentDet> arl=new ArrayList<StudentDet>();
	public static FileOutputStream fos;
	public static ObjectOutputStream dos;
	public static FileInputStream fis;
	public static ObjectInputStream  ois;
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		int choice;
		
		Scanner sc=new Scanner(System.in);
		try
		{
			
		
		FileOutputStream fos=new FileOutputStream("F:\\Student.txt");
		ObjectOutputStream dos=new ObjectOutputStream(fos);
	    FileInputStream fis=new FileInputStream("F:\\student.txt");	
		ObjectInputStream ois=new ObjectInputStream(fis);
	    //ArrayList<StudentDet> arl=new ArrayList<StudentDet>();
	    
		
		System.out.println("1.Store data into file");
	    System.out.println("2.read whos age is > 18 and write into another file");
	    System.out.println("exit");
		System.out.println("enter choice");
		choice=sc.nextInt();
		
		StudentOperation so=new StudentOperation();
	      
	   // sdt.accept();
	    
		/*StudentDet sd=new StudentDet();
		StudentDet sd1=new StudentDet(1,10,220406,"a");
		StudentDet sd2=new StudentDet(4,15,220406,"am");
		StudentDet sd3=new StudentDet(3,33,220406,"amber");
		StudentDet sd4=new StudentDet(5,23,220406,"er");
		StudentDet sd5=new StudentDet(6,19,220406,"amer");
		StudentDet sd6=new StudentDet(2,20,220406,"ambr");
		*/
		
		switch(choice)
		{
		case 1:so.store(); 
			break;
		
		case 2:	so.Readstore();
		break;
		
		default:System.out.println("invalid choice");
		break;	
		}
		}
		catch(Exception e)
		{
			System.out.println(e.getMessage());
		}
	  
	}
	StudentDet sdt=new StudentDet();
	
	public  void Readstore()
	{    try
	{
		
	
		FileOutputStream ffff=new FileOutputStream("F:\\newstudent.txt");
		ObjectOutputStream oooo=new ObjectOutputStream(ffff);
		for(Object obb:arl)
		{
		if(((StudentDet)obb).getAge()>18)
		{
			System.out.println(obb.toString());
			oooo.writeObject(obb);
		}
		}
	}
	catch (Exception e)
	{
		System.out.println(e.getMessage());
	}
	}
	public void store()
	{
		try
		{
			
			for(int i=0;i<2;i++)
			{
			sdt.accept();
			}
			arl.add(sdt);
		       for(Object ob:arl)
		    	   dos.writeObject(ob);
		      
			
		    	
			
		}
		catch(Exception e)
		{
			System.out.println(e.getMessage());
		}
	}

}
