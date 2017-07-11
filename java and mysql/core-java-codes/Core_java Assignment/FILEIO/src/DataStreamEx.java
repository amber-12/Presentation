import java.io.DataOutputStream;
import java.io.FileOutputStream;
import java.util.ArrayList;
import java.util.List;


public class DataStreamEx {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
try
{
	FileOutputStream fops=new FileOutputStream("F:\\datafile.txt");
	DataOutputStream dis=new DataOutputStream(fops);
	
/*	List<StudentDetails> lst=new ArrayList<StudentDetails>();
	lst.add(new StudentDetails("amber",24,840000,'M'));
	lst.add(new StudentDetails("gaurav",29,7000,'M'));
	lst.add(new StudentDetails("akshay",24,66000,'M'));
	lst.add(new StudentDetails("anil",23,22000,'M'));
	lst.add(new StudentDetails("rekha",19,8000,'F'));
	lst.add(new StudentDetails("tanvi",29,79000,'F'));
*/	
	StudentDetails ob1=new StudentDetails("amber",24,80000,'M');
	StudentDetails ob2=new StudentDetails("Gaurav",22,8000,'M');
	StudentDetails ob3=new StudentDetails("Akshay",26,8099000,'M');
	StudentDetails ob4=new StudentDetails("James",27,809000,'M');
	StudentDetails ob5=new StudentDetails("radhika",23,90000,'F');
	StudentDetails ob6=new StudentDetails("Tanvi",24,80000,'F');
	dis.writeUTF(ob1.getSname());
	dis.writeInt(ob1.getSage());
	dis.writeFloat(ob1.getFee());
	dis.write(ob1.getGender());
	
	dis.writeUTF(ob2.getSname());
	dis.writeInt(ob2.getSage());
	dis.writeFloat(ob2.getFee());
	dis.write(ob2.getGender());
	
	dis.writeUTF(ob3.getSname());
	dis.writeInt(ob3.getSage());
	dis.writeFloat(ob3.getFee());
	dis.write(ob3.getGender());
	
	dis.writeUTF(ob4.getSname());
	dis.writeInt(ob4.getSage());
	dis.writeFloat(ob4.getFee());
	dis.write(ob4.getGender());
	
	dis.writeUTF(ob5.getSname());
	dis.writeInt(ob5.getSage());
	dis.writeFloat(ob5.getFee());
	dis.write(ob5.getGender());
	
	
	
}
catch (Exception e)
{
	
}
	}

}
