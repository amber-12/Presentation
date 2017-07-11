import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;


public class JavaIOEX {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		int i;
		char c;
		try
		{
			FileInputStream f=new FileInputStream("F:\\All in One.txt");
		while((i=f.read())!=-1)
		{
			c=(char)i;
			System.out.println(c);
		}
		
		}
		catch(IOException e)
		{
			System.out.println(e.getMessage());
			
		}
		String s="wecome to java";
		byte b[]=s.getBytes();
		try
		{
        FileOutputStream wf=new FileOutputStream("F:\\All in One.txt");
        
        wf.write(b);
        wf.close();
		}
		
		catch(Exception e)
		{
			System.out.println(e.getMessage());
			System.out.println("sucess");
		}
	}

}
