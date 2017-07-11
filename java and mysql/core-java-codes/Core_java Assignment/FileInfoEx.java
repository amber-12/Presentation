import java.io.File;
import java.io.IOException;


public class FileInfoEx {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		try
		{
			File fobj=new File("F:\\All in One.txt");
			System.out.println("check file can be read by application");
			System.out.println(fobj.canRead());
			
			System.out.println("check file can be modified by appn");
			System.out.println(fobj.canWrite());
			
			System.out.println("check file or directory exist");
			System.out.println(fobj.exists());
			
			System.out.println("get absulate path");
			System.out.println(fobj.getAbsolutePath());
			
			System.out.println("Check hidden or not");
			System.out.println(fobj.isHidden());
		}
		catch(Exception e1)
		{
			System.out.println(e1.getMessage());
		}

	}

}
