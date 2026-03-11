"use client";
import { useEffect } from "react";
import { useRouter } from "next/navigation";

export default function LoginPage() {
  const router = useRouter();

  useEffect(() => {
    async function check() {
      sessionStorage.setItem("jobscan_user_id", "5");
      try {
        const res = await fetch("/api/user/5");
        const data = await res.json();
        if (data.exists) {
          router.push("/app?user_id=5");
        } else {
          router.push("/app?user_id=5&scan=1");
        }
      } catch {
        router.push("/app?user_id=5");
      }
    }
    check();
  }, []);

  return null;
}