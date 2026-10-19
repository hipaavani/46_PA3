#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/wait.h>

#define MAX_LINE 1024
#define MAX_ARGS 64

static int parse_command(char *line, char *args[])
{
    int argc = 0;
    char *token = strtok(line, " \t\r\n");

    while (token != NULL && argc < MAX_ARGS - 1) {
        args[argc++] = token;
        token = strtok(NULL, " \t\r\n");
    }

    args[argc] = NULL;
    return argc;
}

static void execute_external(char *args[])
{
    (void)args;

    pid_t pid = fork();

    if (pid < 0) {
        perror("fork");
        return;
    }

    if (pid == 0) {
        
        execvp(args[0], args);
        perror(args[0]);   
        _exit(1);          
    }

    /* parent: wait for that specific child */
    int status;
    while (waitpid(pid, &status, 0) == -1) {
        if (errno != EINTR) {
            perror("waitpid");
            break;
        }
    }

}

int main(void)
{
    char line[MAX_LINE];
    char *args[MAX_ARGS];

    while (1) {
        printf("msh> ");
        fflush(stdout);

        if (fgets(line, sizeof(line), stdin) == NULL) {
            putchar('\n');
            break;
        }

        int argc = parse_command(line, args);

        if (argc == 0) {
            continue;
        }

        if (strcmp(args[0], "exit") == 0) {
            exit(0);
        }

        if (strcmp(args[0], "cd") == 0) {
            if (argc != 2) {
                fprintf(stderr, "cd: expected exactly one argument\n");
            } else if (chdir(args[1]) != 0) {
                perror("cd");
            }
            continue;
        }

        execute_external(args);
    }

    return 0;
}
