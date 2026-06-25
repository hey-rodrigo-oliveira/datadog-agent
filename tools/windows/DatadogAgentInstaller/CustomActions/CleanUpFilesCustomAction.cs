using Datadog.CustomActions.Extensions;
using Datadog.CustomActions.Interfaces;
using WixToolset.Dtf.WindowsInstaller;
using System;
using System.IO;
using System.Linq;

namespace Datadog.CustomActions
{
    public class CleanUpFilesCustomAction
    {
        private static ActionResult CleanupFiles(ISession session)
        {
            var projectLocation = session.Property("PROJECTLOCATION");
            var toDelete = new[]
            {
                // may contain python files created outside of install
                Path.Combine(projectLocation, "embedded2"),
                Path.Combine(projectLocation, "embedded3"),
                Path.Combine(projectLocation, "python-scripts"),
            }
            // installation specific files
            .Concat(session.GeneratedPaths())
            .Concat(session.InstallLocationGeneratedPaths());

            foreach (var path in toDelete)
            {
                try
                {
                    if (Directory.Exists(path))
                    {
                        session.Log($"Deleting directory \"{path}\"");
                        Directory.Delete(path, true);
                    }
                    else if (File.Exists(path))
                    {
                        session.Log($"Deleting file \"{path}\"");
                        File.Delete(path);
                    }
                    else
                    {
                        session.Log($"{path} not found, skip deletion.");
                    }
                }
                catch (Exception e)
                {
                    session.Log($"Error while deleting file: {e}");
                    // Don't fail in cleanup/rollback actions otherwise
                    // we may brick the installation.
                }
            }

            TryRemoveEmptyProcessesD(session, projectLocation);

            return ActionResult.Success;
        }

        private static void TryRemoveEmptyProcessesD(ISession session, string projectLocation)
        {
            if (session.Property("RemoveEmptyProcessesD") != "1")
            {
                return;
            }

            var processesDir = Path.Combine(projectLocation, "processes.d");
            try
            {
                if (!Directory.Exists(processesDir))
                {
                    return;
                }

                if (Directory.EnumerateFileSystemEntries(processesDir).Any())
                {
                    session.Log($"{processesDir} is not empty, skip deletion.");
                    return;
                }

                session.Log($"Deleting empty directory \"{processesDir}\"");
                Directory.Delete(processesDir);
            }
            catch (Exception e)
            {
                session.Log($"Error while deleting empty processes.d directory: {e}");
            }
        }

        public static ActionResult CleanupFiles(Session session)
        {
            return CleanupFiles(new SessionWrapper(session));
        }
    }
}
