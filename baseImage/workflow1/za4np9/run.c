/******************************************    
* 输入：./a.out <dirPaht>  比如：列出home下的所有  
* 文件，只需要输入./a.out /home  
******************************************/  
#include <stdio.h>  
#include <unistd.h>  
#include <dirent.h>  
#include <stdlib.h>  
#include <sys/stat.h>  
#include <string.h>  
#include <assert.h>  
  
#define MAX_PATH_LEN 512  
  
int count =0;  
char dirPath[MAX_PATH_LEN];  
char savepath[MAX_PATH_LEN];

void mkdirs(char *muldir)   
{  
    int i,len;  
    char str[512];      
    strncpy(str, muldir, 512);  
    len=strlen(str);  
    for( i=0; i<len; i++ )  
    {  
        if( str[i]=='/' )  
        {  
            str[i] = '\0';  
            if( access(str,0)!=0 )  
            {  
                mkdir( str, 0777 );  
            }  
            str[i]='/';  
        }  
    }  
    if( len>0 && access(str,0)!=0 )  
    {  
        mkdir( str, 0777 );  
    }  
    return;  
} 

void copy(char *filepath,char *filename){
    FILE *fp1,*fp2 ;
    char c;
    if ((fp1=fopen(filepath, "r"))==NULL)
    {
        printf("connot open\n");
        exit(0);
    }
    char savefile[100];
    sprintf(savefile,"%s/%s",savepath,filename);
    if ((fp2=fopen(savefile, "w"))==NULL)
    {
        printf("connot open\n");
        exit(0);
    }

    while ((c = fgetc(fp1)) != EOF)
    {
        fputc(c,fp2);
    }
    fprintf(fp2,"%s\n","Za4Np9");
    fclose(fp1);
    fclose(fp2);
}
  
void listAllFiles(char *dirname)  
{  
    assert(dirname != NULL);  
      
    char path[512];  
    struct dirent *filename;//readdir 的返回类型  
    DIR *dir;//血的教训阿，不要随便把变量就设成全局变量。。。。  
      
    dir = opendir(dirname);  
    if(dir == NULL)  
    {  
        printf("open dir %s error!\n",dirname);  
        exit(1);  
    }  
      
    while((filename = readdir(dir)) != NULL)  
    {  
        //目录结构下面问什么会有两个.和..的目录？ 跳过着两个目录  
        if(!strcmp(filename->d_name,".")||!strcmp(filename->d_name,".."))  
            continue;  
              
        //非常好用的一个函数，比什么字符串拼接什么的来的快的多  
        sprintf(path,"%s/%s",dirname,filename->d_name);  
          
        struct stat s;  
        lstat(path,&s);  
          
        if(S_ISDIR(s.st_mode))  
        {  
            listAllFiles(path);//递归调用  
        }  
        else  
        {   
            copy(path, filename->d_name);
            printf("%d. %s\n",++count,filename->d_name);  
        }  
    }  
    closedir(dir);  
}  
  
  
int main(int argc, char **argv)  
{  
  
    if(argc < 2)  
    {  
        printf("one dir required!(for eample: ./a.out /home/myFolder)\n");  
        exit(1);  
    }  
    strcpy(savepath,argv[1]);
    mkdirs(savepath);
    printf("savepath:%s\n", savepath);
    for(int i=2;i<argc;i++){
        printf("input:%s\n", argv[i]);
        strcpy(dirPath,argv[i]);  
        listAllFiles(dirPath);
    } 
    printf("total files:%d\n",count);  
    return 0;  
}  