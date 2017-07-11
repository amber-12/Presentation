import java.io.FileWriter;
import java.io.*;

public class WriteFileEx {

	
	public static void main(String[] args) {
		// TODO Auto-generated method stub
       
		String fname="F:\\myfile.txt";
		FileWriter fw=null;
		BufferedWriter bf= null;
	try
	{
		fw=new FileWriter(fname);
		bf=new BufferedWriter(fw);
		bf.write("hi this is my buffered reader ");
		bf.write("this is second line");
		bf.write("hi am amber ");
		bf.write("hello");
		
		bf.close();
		fw.close();
	}
	catch(Exception e)
	{
		System.out.println(e.getMessage());
	}
	
	finally
	{
		System.out.println("Sucess");
		try
		{
			if(bf!=null)
			{
				bf.close();
			}
			else if(fw!=null)
			{
				fw.close();
			}
		}
		catch(Exception e1)
		{
			System.out.println(e1.getMessage());
		}
	}
	}

}
